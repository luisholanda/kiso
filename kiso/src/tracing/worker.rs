use std::{
    collections::HashMap,
    pin::Pin,
    time::{Duration, Instant},
};

use flume::{Receiver, Sender};
use opentelemetry::{
    trace::{Event, SpanId, Status, TraceId},
    InstrumentationLibrary, Key, KeyValue,
};
use opentelemetry_sdk::{
    export::trace::{SpanData, SpanExporter},
    trace::{SpanEvents, SpanLimits},
};
use tokio::{task::JoinHandle, time::Sleep};
use tracing::{Level, Metadata};

pub(super) enum Command {
    EmitSpan(Box<SpanData>, &'static Metadata<'static>),
    EmitEvent(Event, TraceId, SpanId, &'static Metadata<'static>),
}

impl Command {
    pub(super) fn is_important(&self) -> bool {
        match self {
            Self::EmitEvent(_, _, _, meta) => *meta.level() > Level::DEBUG,
            _ => true,
        }
    }
}

type LightSpanCtx = (TraceId, SpanId);

pub(super) struct Worker<'t, SE> {
    rx: Receiver<Command>,
    library: InstrumentationLibrary,
    events: HashMap<LightSpanCtx, SpanEvents>,
    spans: Vec<SpanData>,
    force_export_timer: Pin<&'t mut Sleep>,
    config: WorkerConfig<SE>,
    export_task: Option<JoinHandle<()>>,
}

pub(super) struct WorkerConfig<SE> {
    pub(super) span_exporter: SE,
    pub(super) cmd_channel_capacity: usize,
    pub(super) span_limits: SpanLimits,
    pub(super) spans_batch_max_size: usize,
    pub(super) spans_batch_max_delay: Duration,
}

impl<SE: SpanExporter + 'static> Worker<'_, SE> {
    pub(super) fn spawn(config: WorkerConfig<SE>) -> Sender<Command> {
        let (tx, rx) = flume::bounded(config.cmd_channel_capacity);

        crate::rt::spawn(async move {
            let instrumentation_library = InstrumentationLibrary::builder(env!("CARGO_PKG_NAME"))
                .with_version(env!("CARGO_PKG_VERSION"))
                .with_schema_url("https://opentelemetry.io/schemas/1.35.0")
                .build();

            let mut worker = Worker {
                rx,
                library: instrumentation_library,
                events: HashMap::with_capacity(1024),
                force_export_timer: std::pin::pin!(tokio::time::sleep(
                    config.spans_batch_max_delay
                )),
                spans: Vec::with_capacity(config.spans_batch_max_size),
                config,
                export_task: None,
            };

            loop {
                tokio::select! {
                    Ok(cmd) = worker.rx.recv_async() => {
                        worker.handle_command(cmd);

                        while let Ok(cmd) = worker.rx.try_recv() {
                            worker.handle_command(cmd);
                        }
                    }
                    _ = worker.force_export_timer.as_mut() => {
                        worker.wait_for_export().await;
                        worker.start_spans_export();
                    }
                }

                if worker.spans.len() >= worker.config.spans_batch_max_size {
                    worker.wait_for_export().await;
                    worker.start_spans_export();
                }
            }
        });

        tx
    }

    async fn wait_for_export(&mut self) {
        if let Some(task) = self.export_task.take() {
            let _ = task.await;
        }
    }

    fn start_spans_export(&mut self) {
        if self.spans.is_empty() {
            return;
        }

        let spans = std::mem::replace(
            &mut self.spans,
            Vec::with_capacity(self.config.spans_batch_max_size),
        );

        let export_fut = self.config.span_exporter.export(spans);
        self.export_task = Some(tokio::spawn(async move {
            let _ = export_fut.await;
        }));

        self.force_export_timer
            .as_mut()
            .reset((Instant::now() + self.config.spans_batch_max_delay).into());
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::EmitSpan(mut span, meta) => {
                self.clean_span_data_from_layer(&mut span, meta);

                span.events = self
                    .events
                    .remove(&(span.span_context.trace_id(), span.span_context.span_id()))
                    .unwrap_or_default();

                self.spans.push(*span);
            }
            Command::EmitEvent(mut event, trace_id, span_id, meta) => {
                self.clean_event_from_layer(&mut event, meta);

                let events = self.events.entry((trace_id, span_id)).or_default();
                if events.len() > self.config.span_limits.max_events_per_span as usize {
                    events.dropped_count += 1;
                } else {
                    events.events.push(event);
                }
            }
        }
    }

    fn clean_span_data_from_layer(&self, span: &mut SpanData, meta: &'static Metadata<'static>) {
        let mut idx = 0;
        while idx < span.attributes.len() {
            let attr = &span.attributes[idx];
            match attr.key.as_str() {
                "otel.status_message" | "error" => {
                    let msg = span.attributes.swap_remove(idx).value.as_str().into_owned();
                    span.status = opentelemetry::trace::Status::error(msg);
                }
                _ => idx += 1,
            }
        }

        if let Some(module) = meta.module_path() {
            span.attributes
                .push(KeyValue::new("code.namespace", module));
        }

        if let Some(filepath) = meta.file() {
            span.attributes
                .push(KeyValue::new("code.filepath", filepath));
        }

        if let Some(line) = meta.line() {
            span.attributes
                .push(KeyValue::new("code.lineno", line as i64));
        }

        let span_limits = self.config.span_limits;

        span.dropped_attributes_count =
            span.attributes
                .len()
                .saturating_sub(span_limits.max_attributes_per_span as usize) as u32;
        span.attributes
            .truncate(span_limits.max_attributes_per_span as _);

        span.links.dropped_count =
            span.links
                .len()
                .saturating_sub(span_limits.max_links_per_span as usize) as u32;
        span.links
            .links
            .truncate(span_limits.max_links_per_span as _);

        for link in &mut span.links.links {
            link.dropped_attributes_count = link
                .attributes
                .len()
                .saturating_sub(span_limits.max_attributes_per_link as usize)
                as u32;
            link.attributes
                .truncate(span_limits.max_attributes_per_link as _);
        }

        span.instrumentation_lib.clone_from(&self.library);
    }

    fn clean_event_from_layer(&self, event: &mut Event, meta: &'static Metadata<'static>) {
        let mut idx = 0;
        let mut is_log = false;
        while idx < event.attributes.len() {
            let attr = &mut event.attributes[idx];
            match attr.key.as_str() {
                "log.target" => {
                    is_log = true;
                    attr.key = Key::from_static_str("target");
                }
                "log.module_path" => {
                    is_log = true;
                    attr.key = Key::from_static_str("code.namespace");
                }
                "log.file" => {
                    is_log = true;
                    attr.key = Key::from_static_str("code.filepath");
                }
                "log.line" => {
                    is_log = true;
                    attr.key = Key::from_static_str("code.lineno");
                }
                "error" => {
                    event.name = "exception".into();
                    attr.key = Key::from_static_str("exception.message");
                }
                "error.chain" => attr.key = Key::from_static_str("exception.stacktrace"),
                _ => {}
            }

            idx += 1;
        }

        if !is_log {
            if let Some(module) = meta.module_path() {
                event
                    .attributes
                    .push(KeyValue::new("code.namespace", module));
            }

            if let Some(filepath) = meta.file() {
                event
                    .attributes
                    .push(KeyValue::new("code.filepath", filepath));
            }

            if let Some(line) = meta.line() {
                event
                    .attributes
                    .push(KeyValue::new("code.lineno", line as i64));
            }

            event
                .attributes
                .push(KeyValue::new("target", meta.target()));
        }

        // level is too important to be removed when checking limits.
        event
            .attributes
            .insert(0, KeyValue::new("level", meta.level().as_str()));

        let span_limits = self.config.span_limits;

        event.dropped_attributes_count = event
            .attributes
            .len()
            .saturating_sub(span_limits.max_attributes_per_event as usize)
            as u32;
        event
            .attributes
            .truncate(span_limits.max_attributes_per_event as _);
    }
}
