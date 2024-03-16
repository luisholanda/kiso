use std::{
    borrow::Cow,
    collections::HashMap,
    time::{Duration, SystemTime},
};

use backtrace::Backtrace;
use flume::{Receiver, Sender};
use opentelemetry::{
    logs::{LogRecord, Severity},
    trace::{Link, SpanId, Status, TraceId},
    InstrumentationLibrary, KeyValue,
};
use opentelemetry_sdk::{
    export::{logs::LogData, trace::SpanData},
    logs::{BatchLogProcessor, LogProcessor},
    resource::*,
    runtime::Tokio,
    trace::{SpanEvents, SpanLinks},
    Resource,
};
use opentelemetry_semantic_conventions::trace;

use super::{logging::SourceLocation, tracing::NewSpan};

pub(super) enum Command {
    // Logging
    EmitLog(LogRecord, SourceLocation, Option<Backtrace>),

    // Tracing
    NewSpan(NewSpan),
    SetSpanAttr(LightSpanCtx, KeyValue),
    SetSpanStatus(LightSpanCtx, Status),
    UpdateSpanName(LightSpanCtx, Cow<'static, str>),
    AddLinkToSpan(LightSpanCtx, Link),
    EndSpan(LightSpanCtx, SystemTime),
}

impl Command {
    pub(super) fn must_be_sent(&self) -> bool {
        match self {
            Self::EmitLog(
                LogRecord {
                    severity_number: Some(n),
                    ..
                },
                _,
                _,
            ) => *n >= Severity::Info,
            _ => true,
        }
    }
}

pub(super) type LightSpanCtx = (TraceId, SpanId);

pub(super) struct Worker {
    rx: Receiver<Command>,
    log_processor: BatchLogProcessor<Tokio>,
    log_backtrace_printer: Box<dyn Fn(Backtrace) -> String>,
    resource: &'static Resource,
    library: InstrumentationLibrary,
    spans: HashMap<LightSpanCtx, SpanData>,
}

pub(super) struct WorkerConfig {
    pub(super) log_processor: BatchLogProcessor<Tokio>,
    pub(super) log_backtrace_printer: Box<dyn Fn(Backtrace) -> String + Send>,
    pub(super) cmd_channel_capacity: usize,
    pub(super) resource_detection_timeout: Duration,
    pub(super) initial_spans_capacity: usize,
}

impl Worker {
    pub(super) fn spawn(config: WorkerConfig) -> Sender<Command> {
        let (tx, rx) = flume::bounded(config.cmd_channel_capacity);

        tokio::task::spawn_blocking(move || {
            let resource = detect_resource(config.resource_detection_timeout);
            let instrumentation_library = InstrumentationLibrary::new(
                env!("CARGO_PKG_NAME"),
                Some(env!("CARGO_PKG_VERSION")),
                Some("https://opentelemetry.io/schemas/1.17.0"),
                None,
            );

            let mut worker = Worker {
                rx,
                log_processor: config.log_processor,
                log_backtrace_printer: config.log_backtrace_printer,
                resource,
                library: instrumentation_library,
                spans: HashMap::with_capacity(config.initial_spans_capacity),
            };

            while let Ok(cmd) = worker.rx.recv() {
                worker.handle_command(cmd);
            }
        });

        tx
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::EmitLog(mut log, loc, bt) => {
                log.attributes
                    .get_or_insert_with(|| Vec::with_capacity(8))
                    .extend([
                        (trace::CODE_LINENO.into(), loc.line.into()),
                        (trace::CODE_COLUMN.into(), loc.column.into()),
                        (trace::CODE_FILEPATH.into(), loc.file.into()),
                        (trace::CODE_FUNCTION.into(), loc.function.into()),
                        (trace::CODE_NAMESPACE.into(), loc.module.into()),
                    ]);

                if let Some(mut bt) = bt {
                    bt.resolve();
                    let bt = (self.log_backtrace_printer)(bt);
                    log.attributes
                        .as_mut()
                        .unwrap()
                        .push((trace::EXCEPTION_STACKTRACE.into(), bt.into()));
                }

                self.log_processor.emit(LogData {
                    record: log,
                    resource: Cow::Borrowed(self.resource),
                    instrumentation: self.library.clone(),
                });
            }
            Command::NewSpan(ns) => {
                let light_span_ctx = (ns.span_context.trace_id(), ns.span_context.span_id());
                self.spans.insert(
                    light_span_ctx,
                    SpanData {
                        span_context: ns.span_context,
                        parent_span_id: ns.parent_id,
                        span_kind: ns.kind,
                        name: ns.name,
                        start_time: ns.start_time,
                        end_time: ns.start_time,
                        resource: Cow::Borrowed(self.resource),
                        instrumentation_lib: self.library.clone(),
                        attributes: ns.attributes,
                        dropped_attributes_count: 0,
                        events: SpanEvents::default(),
                        links: SpanLinks::default(),
                        status: opentelemetry::trace::Status::Ok,
                    },
                );
            }
            Command::SetSpanAttr(span_ctx, attr) => {
                if let Some(span) = self.spans.get_mut(&span_ctx) {
                    span.attributes.push(attr);
                }
            }
            Command::SetSpanStatus(span_ctx, status) => {
                if let Some(span) = self.spans.get_mut(&span_ctx) {
                    span.status = status;
                }
            }
            Command::UpdateSpanName(span_ctx, name) => {
                if let Some(span) = self.spans.get_mut(&span_ctx) {
                    span.name = name;
                }
            }
            Command::AddLinkToSpan(span_ctx, link) => {
                if let Some(span) = self.spans.get_mut(&span_ctx) {
                    span.links.links.push(link);
                }
            }
            Command::EndSpan(span_ctx, end_ts) => {
                if let Some(span) = self.spans.get_mut(&span_ctx) {
                    span.end_time = end_ts;
                    // TODO: dispatch to span processor.
                }
            }
        }
    }
}

fn detect_resource(timeout: Duration) -> &'static Resource {
    let resource = Resource::from_detectors(
        timeout,
        vec![
            Box::new(EnvResourceDetector::new()),
            Box::new(OsResourceDetector),
            Box::new(ProcessResourceDetector),
            Box::new(SdkProvidedResourceDetector),
            Box::new(TelemetryResourceDetector),
        ],
    );

    Box::leak(Box::new(resource))
}
