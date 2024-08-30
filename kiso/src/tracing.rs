use std::{str::FromStr, sync::OnceLock, time::Duration};

use ::tracing::Subscriber;
use flume::Sender;
use opentelemetry::{
    trace::{Link, SpanContext, SpanId, TraceFlags, TraceId, TraceState},
    KeyValue,
};
use opentelemetry_sdk::{
    export::trace::{SpanData, SpanExporter},
    trace::ShouldSample,
};
use tracing_subscriber::{
    filter::{LevelFilter, Targets},
    layer::Layer,
    prelude::*,
    util::SubscriberInitExt,
    EnvFilter,
};

mod layer;
pub(crate) mod worker;

/// Context for propagating tracing information.
#[derive(Clone)]
pub struct TraceContext {
    span_ctx: SpanContext,
}

impl TraceContext {
    /// Try accessing the current context.
    ///
    /// Returns `None` if no context is available.
    pub fn with<T>(f: impl FnOnce(&TraceContext) -> T) -> Option<T> {
        Self::with_span(&tracing::Span::current(), f)
    }

    /// Try accessing the context for a specific span.
    ///
    /// Returns `None` if no context is available.
    pub fn with_span<T>(span: &tracing::Span, f: impl FnOnce(&TraceContext) -> T) -> Option<T> {
        layer::access_span_data(span, |c| f(c))
    }

    /// Access the stored OpenTelemetry span context.
    pub fn span_context(&self) -> &SpanContext {
        &self.span_ctx
    }

    /// Encodes the context into a traceparent header.
    pub fn to_w3c_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.span_ctx.trace_id(),
            self.span_ctx.span_id(),
            self.span_ctx.trace_flags() & TraceFlags::SAMPLED
        )
    }
}

/// Context about the remote tracing context that initialized a request.
///
/// This context is used by the tracing integration to correctly pass
/// trace information to spans.
#[derive(Clone)]
pub struct RemoteContext(pub TraceContext);

crate::impl_context_for_type!(RemoteContext);

impl RemoteContext {
    /// Decodes a remote context from the W3C traceparent + tracestate headers.
    ///
    /// If no tracestate is available, pass an empty string.
    pub fn from_w3c_traceparent(traceparent: &str, tracestate: &str) -> Option<Self> {
        let mut parts = traceparent.split_terminator('-');

        let version = u8::from_str_radix(parts.next()?, 16).ok()?;
        if version == u8::MAX {
            return None;
        }

        let trace_id_hex = parts.next()?;
        if trace_id_hex.chars().any(|c| c.is_ascii_uppercase()) {
            return None;
        }

        let trace_id = TraceId::from_hex(trace_id_hex).ok()?;

        let span_id_hex = parts.next()?;
        if span_id_hex.chars().any(|c| c.is_ascii_uppercase()) {
            return None;
        }

        let span_id = SpanId::from_hex(span_id_hex).ok()?;

        let opts = u8::from_str_radix(parts.next()?, 16).ok()?;

        let trace_flags = TraceFlags::new(opts) & TraceFlags::SAMPLED;

        let tracestate = TraceState::from_str(tracestate).unwrap_or_default();

        if parts.next().is_some() {
            return None;
        }

        Some(Self(TraceContext {
            span_ctx: SpanContext::new(trace_id, span_id, trace_flags, true, tracestate),
        }))
    }
}

/// Allow recording dynamic attributes on spans.
///
/// The function will only be called if the span is enabled and being sampled.
pub fn add_dynamic_attributes<I>(span: &tracing::Span, f: impl FnOnce() -> I)
where
    I: IntoIterator<Item = KeyValue>,
{
    layer::access_span_data::<Box<SpanData>, _>(span, |data| data.attributes.extend(f()));
}

/// Add a link to the given span.
///
/// The function will only be called if the span is enabled and being sampled.
pub fn add_link(span: &tracing::Span, f: impl FnOnce() -> Link) {
    layer::access_span_data::<Box<SpanData>, _>(span, |data| data.links.links.push(f()));
}

/// Exporters used by the kiso's observability stack.
pub struct Exporters<SE, SS> {
    /// Exporter used for spans.
    pub span_exporter: SE,
    /// How spans should be sampled .
    pub span_sampler: SS,
}

/// Initializes kiso's tracing stack.
///
/// # Panics
///
/// Panics if called twice.
pub fn initialize<SE, SS>(exporters: Exporters<SE, SS>)
where
    SE: SpanExporter + 'static,
    SS: ShouldSample + 'static,
{
    let settings: TracingSettings = crate::settings::get();

    let mut log_tracer_builder = tracing_log::LogTracer::builder()
        .with_interest_cache(tracing_log::InterestCacheConfig::default());

    let subscriber = tracing_subscriber::registry();

    if settings.observability_tracing_pretty {
        let fmt = tracing_subscriber::fmt::layer()
            .with_ansi(true)
            .with_level(true)
            .with_target(true)
            .with_line_number(true)
            .pretty();

        let fmt_subscriber = subscriber.with(fmt);

        if let Some(filter) = get_filter(
            settings.observability_tracing_use_targets,
            &mut log_tracer_builder,
        ) {
            fmt_subscriber.with(filter).init();
        } else {
            fmt_subscriber.init();
        }
    } else {
        let subscriber = subscriber.with(layer::KisoTracingLayer {
            id_gen: Default::default(),
            span_limits: Default::default(),
            sampler: exporters.span_sampler,
        });

        if let Some(filter) = get_filter(
            settings.observability_tracing_use_targets,
            &mut log_tracer_builder,
        ) {
            subscriber.with(filter).init();
        } else {
            subscriber.init();
        }

        let sender = worker::Worker::spawn(worker::WorkerConfig {
            span_exporter: exporters.span_exporter,
            cmd_channel_capacity: settings.observability_tracing_worker_buffer_size,
            spans_batch_max_size: settings.observability_span_batch_max_export_batch_size,
            spans_batch_max_delay: settings.observability_span_batch_scheduled_delay,
            span_limits: Default::default(),
        });

        let _ = CMD_CHANNEL.set(sender);
    }

    log_tracer_builder
        .init()
        .expect("failed to set tracing_log log::Log as global logger");
}

fn get_filter<S: Subscriber>(
    use_targets: bool,
    log_tracer_builder: &mut tracing_log::log_tracer::Builder,
) -> Option<Box<dyn Layer<S> + Send + Sync + 'static>> {
    let Ok(rust_log) = std::env::var(EnvFilter::DEFAULT_ENV) else {
        return None;
    };

    let filter = if use_targets {
        let targets: Targets = rust_log.parse().expect("invalid Targets string");

        let mut max_level_filter = targets.default_level().unwrap_or(LevelFilter::INFO);
        for (target, filter) in &targets {
            max_level_filter = max_level_filter.min(filter);

            if filter == LevelFilter::OFF {
                *log_tracer_builder =
                    std::mem::replace(log_tracer_builder, tracing_log::LogTracer::builder())
                        .ignore_crate(target.to_string());
            }
        }

        Layer::boxed(targets)
    } else {
        let env_filter: EnvFilter = rust_log.parse().expect("invalid EnvFilter string");
        Layer::boxed(env_filter)
    };

    Some(filter)
}

crate::settings!(pub(crate) TracingSettings {
    /// Timeout for the background worker to detect all the resource information.
    ///
    /// Defaults to 5s.
    #[arg(value_parser = crate::settings::DurationParser)]
    observability_resource_detection_timeout: Duration = Duration::from_secs(5),
    /// Interval between processing consecutive span batches.
    ///
    /// Defaults to 1s.
    #[arg(value_parser = crate::settings::DurationParser)]
    observability_span_batch_scheduled_delay: Duration = Duration::from_secs(1),
    /// Size of a single spans batch.
    ///
    /// Defaults to 512.
    observability_span_batch_max_export_batch_size: usize = 512,
    /// If we should use the lighter `Targets` filter instead of the more complex
    /// `EnvFilter`.
    ///
    /// Enabled by default.
    observability_tracing_use_targets: bool = true,
    /// If we should pretty print tracing spans and events to stdout instead of
    /// passing to the OpenTelemetry stack.
    ///
    /// This should be used for local development only.
    observability_tracing_pretty: bool = false,
    /// Capacity of the tracing background worker buffer.
    ///
    /// If the buffer is full, non-important events will be dropped.
    ///
    /// Defaults to 4096.
    observability_tracing_worker_buffer_size: usize = 4096,
});

static CMD_CHANNEL: OnceLock<Sender<worker::Command>> = OnceLock::new();

fn send_cmd(cmd: worker::Command) {
    let Some(tx) = CMD_CHANNEL.get() else {
        return;
    };

    if let Err(flume::TrySendError::Full(cmd)) = tx.try_send(cmd) {
        if cmd.is_important() {
            let _ = tokio::task::block_in_place(|| tx.send(cmd));
        }
    }
}
