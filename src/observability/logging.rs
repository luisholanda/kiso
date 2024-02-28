use std::time::SystemTime;

pub use opentelemetry::logs::Severity;
use opentelemetry::{
    logs::{AnyValue, LogRecordBuilder},
    Key,
};

#[doc(hidden)]
#[inline(always)]
pub fn log(level: Severity, body: AnyValue) -> LogBuilder {
    LogBuilder {
        log: LogRecordBuilder::new()
            .with_timestamp(SystemTime::now())
            .with_severity_number(level)
            .with_severity_text(level.name())
            .with_body(body),
    }
}

/// A builder for log records.
///
/// Creates the record when dropped.
pub struct LogBuilder {
    // We don't expose the LogRecordBuilder directly so that the internal representation
    // is kept hidden, allowing us to optimize the hot path.
    log: LogRecordBuilder,
}

// TODO(errors): add method for error attributes.
impl LogBuilder {
    /// Add an attribute to the log.
    pub fn attr<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<Key>,
        V: Into<AnyValue>,
    {
        self.log = std::mem::take(&mut self.log).with_attribute(key, value);
        self
    }
}

impl Drop for LogBuilder {
    fn drop(&mut self) {
        let record = std::mem::take(&mut self.log).build();

        super::send_cmd(super::worker::Command::EmitLog(record));
    }
}

#[macro_export]
macro_rules! debug {
    ($msg: expr $(,$tt: tt)*) => {
        $self::log!($self::Severity::Debug, $msg, $(,$tt)*)
    };
}

#[macro_export]
macro_rules! info {
    ($msg: expr $(,$tt: tt)*) => {
        $self::log!($self::Severity::Info, $msg, $(,$tt)*)
    };
}

#[macro_export]
macro_rules! warn {
    ($msg: expr $(,$tt: tt)*) => {
        $self::log!($self::Severity::Warn, $msg, $(,$tt)*)
    };
}

#[macro_export]
macro_rules! error {
    ($msg: expr $(,$tt: tt)*) => {
        $self::log!($self::Severity::Error, $msg, $(,$tt)*)
    };
}

#[macro_export]
macro_rules! fatal {
    ($msg: expr $(,$tt: tt)*) => {
        $self::log!($self::Severity::Fatal, $msg, $(,$tt)*)
    };
}

#[macro_export]
macro_rules! log {
    ($lvl: expr, $msg: expr $(,$tt: tt)*) => {
        $self::log($lvl, {
            let msg = format_args!($msg, $(,$tt)*);
            if let Some(msg) = msg.as_str() {
                msg.into()
            } else {
                msg.to_string().into()
            }
        })
    };
}
