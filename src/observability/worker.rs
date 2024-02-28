use std::{borrow::Cow, time::Duration};

use flume::{Receiver, Sender};
use opentelemetry::{
    logs::{LogRecord, Severity},
    InstrumentationLibrary,
};
use opentelemetry_sdk::{
    export::logs::LogData,
    logs::{BatchLogProcessor, LogProcessor},
    resource::*,
    runtime::Tokio,
    Resource,
};

pub(super) enum Command {
    EmitLog(LogRecord),
}

impl Command {
    pub(super) fn must_be_sent(&self) -> bool {
        match self {
            Self::EmitLog(LogRecord {
                severity_number: Some(n),
                ..
            }) => *n >= Severity::Warn,
            _ => true,
        }
    }
}

pub(super) struct Worker {
    rx: Receiver<Command>,
    log_processor: BatchLogProcessor<Tokio>,
    resource: &'static Resource,
    library: InstrumentationLibrary,
}

pub(super) struct WorkerConfig {
    pub(super) log_processor: BatchLogProcessor<Tokio>,
    pub(super) cmd_channel_capacity: usize,
    pub(super) resource_detection_timeout: Duration,
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
                resource,
                library: instrumentation_library,
            };

            while let Ok(cmd) = worker.rx.recv() {
                worker.handle_command(cmd);
            }
        });

        tx
    }

    fn handle_command(&mut self, cmd: Command) {
        match cmd {
            Command::EmitLog(log) => self.log_processor.emit(LogData {
                record: log,
                resource: Cow::Borrowed(self.resource),
                instrumentation: self.library.clone(),
            }),
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
