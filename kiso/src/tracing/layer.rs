use std::time::SystemTime;

use opentelemetry::{
    trace::{SamplingDecision, SpanContext, SpanId, SpanKind, TraceFlags, TraceState},
    Key, KeyValue, StringValue,
};
use opentelemetry_sdk::{
    export::trace::SpanData,
    trace::{IdGenerator, RandomIdGenerator, ShouldSample, SpanEvents, SpanLimits, SpanLinks},
};
use tracing::{span, Subscriber};
use tracing_subscriber::{layer::Context, registry::LookupSpan, Layer, Registry};

use super::{RemoteContext, TraceContext};

const SPAN_NAME_FIELD: &str = "otel.name";
const SPAN_KIND_FIELD: &str = "otel.kind";
const SPAN_STATUS_CODE_FIELD: &str = "otel.status_code";

pub(super) fn access_span_data<E: 'static, T>(
    span: &tracing::Span,
    f: impl FnOnce(&mut E) -> T,
) -> Option<T> {
    span.with_subscriber(|(id, dispatcher)| {
        let registry = dispatcher.downcast_ref::<Registry>()?;
        let span = registry.span(id)?;

        let mut exts = span.extensions_mut();
        exts.get_mut::<E>().map(f)
    })
    .flatten()
}

pub(super) struct KisoTracingLayer<S> {
    pub(super) id_gen: RandomIdGenerator,
    pub(super) span_limits: SpanLimits,
    pub(super) sampler: S,
}

impl<S: ShouldSample> KisoTracingLayer<S> {
    fn sample_span(&self, span: &mut SpanData) -> SamplingDecision {
        let sampler_result = self.sampler.should_sample(
            None,
            span.span_context.trace_id(),
            &span.name,
            &span.span_kind,
            &span.attributes,
            &span.links,
        );

        if sampler_result.decision == SamplingDecision::Drop {
            return sampler_result.decision;
        }

        span.attributes.extend(sampler_result.attributes);

        span.span_context = SpanContext::new(
            span.span_context.trace_id(),
            span.span_context.span_id(),
            if sampler_result.decision == SamplingDecision::RecordAndSample {
                span.span_context.trace_flags() | TraceFlags::SAMPLED
            } else {
                span.span_context.trace_flags() & !TraceFlags::SAMPLED
            },
            false,
            if sampler_result.trace_state == TraceState::NONE {
                span.span_context.trace_state().clone()
            } else {
                sampler_result.trace_state
            },
        );

        sampler_result.decision
    }

    fn new_span_context(&self, parent_ctx: Option<TraceContext>) -> TraceContext {
        TraceContext {
            span_ctx: SpanContext::new(
                parent_ctx
                    .as_ref()
                    .map_or_else(|| self.id_gen.new_trace_id(), |c| c.span_ctx.trace_id()),
                self.id_gen.new_span_id(),
                parent_ctx
                    .as_ref()
                    .map_or(TraceFlags::NOT_SAMPLED, |c| c.span_ctx.trace_flags()),
                false,
                parent_ctx
                    .as_ref()
                    .map_or(TraceState::NONE, |c| c.span_ctx.trace_state().clone()),
            ),
        }
    }
}

impl<S, Sampler> Layer<S> for KisoTracingLayer<Sampler>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    Sampler: ShouldSample + 'static,
{
    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        let parent_ctx = ctx
            .lookup_current()
            .and_then(|span_data| span_data.extensions().get::<TraceContext>().cloned())
            .or_else(|| RemoteContext::try_current().map(|c| c.0));

        if parent_ctx
            .as_ref()
            .map_or(false, |c| !c.span_ctx.trace_flags().is_sampled())
        {
            return;
        }

        let parent_span_id = parent_ctx
            .as_ref()
            .map_or(SpanId::INVALID, |c| c.span_ctx.span_id());

        let span_ref = ctx.span(id).expect("invalid span ID given to layer");
        let mut span_ctx = self.new_span_context(parent_ctx);

        let mut span = Box::new(SpanData {
            span_context: span_ctx.span_ctx,
            parent_span_id,
            span_kind: SpanKind::Internal,
            name: attrs.metadata().name().into(),
            start_time: SystemTime::now(),
            end_time: SystemTime::UNIX_EPOCH,
            attributes: Vec::with_capacity(attrs.fields().len()),
            dropped_attributes_count: 0,
            events: SpanEvents::default(),
            links: SpanLinks::default(),
            instrumentation_lib: Default::default(),
            status: opentelemetry::trace::Status::Unset,
        });

        attrs.record(&mut SpanVisitor(&mut span));

        match self.sample_span(&mut span) {
            SamplingDecision::Drop => return,
            SamplingDecision::RecordOnly => span_ctx.span_ctx = span.span_context,
            SamplingDecision::RecordAndSample => {
                span_ctx.span_ctx = span.span_context.clone();
                span_ref.extensions_mut().insert(span);
            }
        }

        span_ref.extensions_mut().insert(span_ctx);
    }

    fn on_record(&self, span: &span::Id, values: &span::Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(span).expect("invalid span ID");
        let mut extensions = span.extensions_mut();
        let Some(span) = extensions.get_mut::<Box<SpanData>>() else {
            return;
        };

        if span.attributes.len() < self.span_limits.max_attributes_per_span as usize {
            span.attributes.reserve(values.len());
            values.record(&mut SpanVisitor(span));
        }
    }

    fn on_close(&self, id: span::Id, ctx: Context<'_, S>) {
        let span = ctx.span(&id).expect("invalid span ID");
        let metadata = span.metadata();
        let Some(mut span) = span.extensions_mut().remove::<Box<SpanData>>() else {
            return;
        };

        span.end_time = SystemTime::now();

        super::send_cmd(super::worker::Command::EmitSpan(span, metadata));
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let Some(parent) = event.parent().and_then(|id| ctx.span(id)).or_else(|| {
            event
                .is_contextual()
                .then(|| ctx.lookup_current())
                .flatten()
        }) else {
            return;
        };

        let (trace_id, span_id) = {
            let extensions = parent.extensions();
            let Some(trace_ctx) = extensions.get::<TraceContext>() else {
                return;
            };

            if !trace_ctx.span_ctx.is_sampled() {
                return;
            }

            (trace_ctx.span_ctx.trace_id(), trace_ctx.span_ctx.span_id())
        };

        let mut otel_event = opentelemetry::trace::Event::with_name(event.metadata().name());
        otel_event
            .attributes
            .reserve(event.metadata().fields().len());

        event.record(&mut EventVisitor(&mut otel_event));

        super::send_cmd(super::worker::Command::EmitEvent(
            otel_event,
            trace_id,
            span_id,
            event.metadata(),
        ));
    }
}

struct SpanVisitor<'s>(&'s mut SpanData);

impl SpanVisitor<'_> {
    fn add_field(&mut self, field: &tracing::field::Field, f: impl FnOnce(Key) -> KeyValue) {
        self.0
            .attributes
            .push(f(Key::from_static_str(field.name())));
    }

    fn add_int_field<T>(&mut self, field: &tracing::field::Field, value: T)
    where
        T: Copy + std::fmt::Display,
        i64: TryFrom<T>,
    {
        self.add_field(field, |k| {
            if let Ok(value) = i64::try_from(value) {
                k.i64(value)
            } else {
                k.string(value.to_string())
            }
        });
    }
}

impl tracing::field::Visit for SpanVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let value = format!("{value:?}");
        match field.name() {
            SPAN_NAME_FIELD => self.0.name = value.into(),
            SPAN_KIND_FIELD | SPAN_STATUS_CODE_FIELD => {
                self.record_str(field, &value);
            }
            _ => self.add_field(field, |k| k.string(value)),
        }
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.add_field(field, |k| k.f64(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.add_field(field, |k| k.i64(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.add_int_field(field, value);
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        self.add_int_field(field, value);
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.add_int_field(field, value);
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.add_field(field, |k| k.bool(value));
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        match field.name() {
            SPAN_NAME_FIELD => self.0.name = value.to_string().into(),
            SPAN_KIND_FIELD => {
                self.0.span_kind = match value {
                    "server" => SpanKind::Server,
                    "client" => SpanKind::Client,
                    "producer" => SpanKind::Producer,
                    "consumer" => SpanKind::Consumer,
                    "internal" => SpanKind::Internal,
                    _ => panic!("invalid span kind {value:?}"),
                }
            }
            SPAN_STATUS_CODE_FIELD => {
                self.0.status = match value {
                    "ok" => opentelemetry::trace::Status::Ok,
                    "error" => opentelemetry::trace::Status::error(""),
                    _ => opentelemetry::trace::Status::Unset,
                }
            }
            _ => self.add_field(field, |k| k.string(value.to_string())),
        }
    }

    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        let mut chain: Vec<StringValue> = vec![];
        let mut next_err = value.source();
        while let Some(err) = next_err {
            chain.push(err.to_string().into());
            next_err = err.source();
        }

        self.record_debug(field, &tracing::field::display(value));
        self.0
            .attributes
            .push(Key::from(format!("{}.chain", field.name())).array(chain));
    }
}

struct EventVisitor<'a>(&'a mut opentelemetry::trace::Event);

impl EventVisitor<'_> {
    fn add_field(&mut self, field: &tracing::field::Field, f: impl FnOnce(Key) -> KeyValue) {
        self.0
            .attributes
            .push(f(Key::from_static_str(field.name())));
    }

    fn add_int_field<T>(&mut self, field: &tracing::field::Field, value: T)
    where
        T: Copy + std::fmt::Display,
        i64: TryFrom<T>,
    {
        self.add_field(field, |k| {
            if let Ok(value) = i64::try_from(value) {
                k.i64(value)
            } else {
                k.string(value.to_string())
            }
        });
    }
}

impl tracing::field::Visit for EventVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.add_field(field, |k| k.string(format!("{value:?}")));
    }

    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        self.add_field(field, |k| k.f64(value));
    }

    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.add_field(field, |k| k.i64(value));
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.add_int_field(field, value);
    }

    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        self.add_int_field(field, value);
    }

    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        self.add_int_field(field, value);
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.add_field(field, |k| k.bool(value));
    }
}
