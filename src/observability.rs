use std::time::Duration;

use flume::{Sender, TrySendError};
use once_cell::sync::OnceCell;
use opentelemetry_sdk::{
    export::logs::LogExporter,
    logs::{BatchConfigBuilder, BatchLogProcessor},
    runtime::Tokio,
};

use self::worker::{Command, Worker, WorkerConfig};

pub mod logging;

mod worker;

pub struct Exporters<L> {
    log_exporter: L,
}

/// Initializes kiso's observability stack.
///
/// # Panics
///
/// Panics if called twice.
pub fn initialize<L>(exporters: Exporters<L>)
where
    L: LogExporter + 'static,
{
    assert!(CMD_SENDER.get().is_none());

    let settings: ObservabilitySettings = crate::settings::get();

    let log_processor_config = BatchConfigBuilder::default()
        .with_max_queue_size(settings.observability_logging_batch_max_queue_size)
        .with_scheduled_delay(settings.observability_logging_batch_scheduled_delay)
        .with_max_export_timeout(settings.observability_logging_batch_max_export_timeout)
        .with_max_export_batch_size(settings.observability_logging_batch_max_export_batch_size)
        .build();

    let sender = Worker::spawn(WorkerConfig {
        log_processor: BatchLogProcessor::builder(exporters.log_exporter, Tokio)
            .with_batch_config(log_processor_config)
            .build(),
        cmd_channel_capacity: settings.observability_buffer_capacity,
        resource_detection_timeout: settings.observability_resource_detection_timeout,
    });

    CMD_SENDER
        .set(sender)
        .expect("observability stack cannot be initialized more than once");
}

crate::settings!(ObservabilitySettings {
    /// The capacity for the buffer used to send observability data to the background worker.
    ///
    /// This is in numbers of commands that the buffer can hold.
    ///
    /// Note that, if the buffer is full, some low priority commands may be lost, like `DEBUG`
    /// logs.
    observability_buffer_capacity: usize = 128 * 1024 / std::mem::size_of::<Command>(),
    /// Timeout for the background worker to detect all the resource information.
    /// Defaults to 5s.
    #[arg(value_parser = crate::settings::DurationParser)]
    observability_resource_detection_timeout: Duration = Duration::from_secs(5),
    ///
    observability_logging_batch_max_queue_size: usize = 2048,
    ///
    #[arg(value_parser = crate::settings::DurationParser)]
    observability_logging_batch_scheduled_delay: Duration = Duration::from_secs(1),
    ///
    #[arg(value_parser = crate::settings::DurationParser)]
    observability_logging_batch_max_export_timeout: Duration = Duration::from_secs(30),
    ///
    observability_logging_batch_max_export_batch_size: usize = 512,
});

static CMD_SENDER: OnceCell<Sender<Command>> = OnceCell::new();

fn send_cmd(cmd: Command) {
    let sender = if cfg!(debug_assertions) {
        CMD_SENDER.get().expect("observability stack not enabled")
    } else {
        // SAFETY: the stack assumes it is enabled.
        unsafe { CMD_SENDER.get_unchecked() }
    };

    if let Err(TrySendError::Full(cmd)) = sender.try_send(cmd) {
        // To reduce pressure in the worker, only send the command if it MUST be sent.
        if cmd.must_be_sent() {
            tokio::spawn(sender.send_async(cmd));
        }
    }
}
