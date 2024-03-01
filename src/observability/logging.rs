use std::time::SystemTime;

use backtrace::Backtrace;
pub use opentelemetry::logs::Severity;
use opentelemetry::{
    logs::{AnyValue, LogRecordBuilder},
    Key,
};

#[doc(hidden)]
#[inline(always)]
pub fn log(level: Severity, body: AnyValue, loc: SourceLocation) -> LogBuilder {
    LogBuilder {
        log: LogRecordBuilder::new()
            .with_timestamp(SystemTime::now())
            .with_severity_number(level)
            .with_severity_text(level.name())
            .with_body(body),
        location: loc,
        backtrace: None,
    }
}

/// A builder for log records.
///
/// Creates the record when dropped.
pub struct LogBuilder {
    // We don't expose the LogRecordBuilder directly so that the internal representation
    // is kept hidden, allowing us to optimize the hot path.
    log: LogRecordBuilder,
    // store the location instead of settings the attributes directly to prevent allocating
    // the attribute vector in the common case where no attributes are set.
    location: SourceLocation,
    backtrace: Option<Backtrace>,
}

// TODO(errors): add method for error attributes.
impl LogBuilder {
    /// Add an attribute to the log.
    ///
    /// No deduplication is made in the attributes. The behavior for duplicates will be
    /// controled by the configured exporter.
    pub fn attr<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<Key>,
        V: Into<AnyValue>,
    {
        self.log = std::mem::take(&mut self.log).with_attribute(key, value);
        self
    }

    /// Add a backtrace to the log.
    ///
    /// This allows a more precise information on where the log was made, but is more
    /// expensive to get.
    ///
    /// Is recommended to pass an unresolved backtrace, as then the resolve process
    /// can be made outside of the hot path.
    pub fn backtrace(mut self, backtrace: Backtrace) -> Self {
        self.backtrace = Some(backtrace);
        self
    }
}

impl Drop for LogBuilder {
    fn drop(&mut self) {
        let record = std::mem::take(&mut self.log).build();

        super::send_cmd(super::worker::Command::EmitLog(
            record,
            std::mem::take(&mut self.location),
        ));
    }
}

#[macro_export]
macro_rules! debug {
    ($msg: expr) => {
        $crate::debug!($msg,)
    };
    ($msg: expr, $($tt: tt)*) => {
        $crate::log!($crate::observability::logging::Severity::Debug, $msg, $($tt)*)
    };
}

#[macro_export]
macro_rules! info {
    ($msg: expr) => {
        $crate::debug!($msg,)
    };
    ($msg: expr, $($tt: tt)*) => {
        $crate::log!($crate::observability::logging::Severity::Info, $msg, $($tt)*)
    };
}

#[macro_export]
macro_rules! warn {
    ($msg: expr) => {
        $crate::debug!($msg,)
    };
    ($msg: expr, $($tt: tt)*) => {
        $crate::log!($crate::observability::logging::Severity::Warn, $msg, $($tt)*)
    };
}

#[macro_export]
macro_rules! error {
    ($msg: expr) => {
        $crate::debug!($msg,)
    };
    ($msg: expr, $($tt: tt)*) => {
        $crate::log!($crate::observability::logging::Severity::Error, $msg, $($tt)*)
    };
}

#[macro_export]
macro_rules! log {
    ($lvl: expr, $msg: literal) => {
        $crate::log!(@ $lvl, {
            let msg = format_args!($msg);

            if let Some(msg) = msg.as_str() {
                msg.into()
            } else {
                msg.to_string().into()
            }
        })
    };
    ($lvl: expr, $msg: literal, $($tt:tt)*) => {
        $crate::log!(@ $lvl, format_args!($msg, $($tt)*).to_string().into())
    };
    (@ $lvl: expr, $body: expr) => {
        $crate::observability::logging::log(
            $lvl,
            $body,
            $crate::observability::logging::SourceLocation {
                line: ::std::line!(),
                column: ::std::column!(),
                file: ::std::file!(),
                module: ::std::module_path!(),
                function: {
                    {
                        fn f() {}
                        let name = ::std::any::type_name_of_val(&f);
                        let name = &name[..name.len() - 3];
                        name.trim_end_matches("::{{closure}}")
                    }
                },
            },
        )
    };
}

#[derive(Default)]
pub struct SourceLocation {
    pub line: u32,
    pub column: u32,
    pub file: &'static str,
    pub module: &'static str,
    pub function: &'static str,
}
