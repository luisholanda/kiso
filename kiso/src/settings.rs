use core::panic;
use std::{ffi::OsStr, marker::PhantomData, time::Duration};

#[doc(hidden)]
pub use clap::Parser as __Parser;
use clap::{builder::TypedValueParser, Command};
use once_cell::sync::OnceCell;

static SETTINGS: OnceCell<Settings> = OnceCell::new();

/// Create a new [`SettingsBuilder`], allowing one to register new
/// settings segments.
pub fn builder(cmd_descriptor: CmdDescriptor) -> SettingsBuilder {
    Settings::builder(cmd_descriptor)
}

/// Get a segment of the application settings represented by `T`.
///
/// # Panics
///
/// Panics, without calling the panic hook, if the segment can be extracted
/// from the command line arguments. In this case an error message will be
/// printed informing the problem.
pub fn get<T: SettingsFromArgs>() -> T {
    SETTINGS.get().expect("Settings not installed").get()
}

/// Descriptor of the command being executed.
#[derive(Clone, Copy, Debug)]
pub struct CmdDescriptor {
    /// The name of the program.
    pub name: &'static str,
    /// A short description about what the program does.
    pub about: &'static str,
}

/// Helper macro to make it easier to define settings struct.
///
/// For examples, see the structs defined in this crate.
#[macro_export]
macro_rules! settings {
    ($(#[$ty_meta: meta])* $vis: vis $ty_name: ident { $($(#[$meta: meta])+ $setting: ident : $ty: ty $(= $default: expr)? ,)+ }) => {
        $(#[$ty_meta])*
        #[derive($crate::settings::__Parser)]
        $vis struct $ty_name {
            $(
                $(#[$meta])+
                #[arg(long, required(false))]
                $vis $setting: $ty,
            )+
        }

        impl ::std::default::Default for $ty_name {
            fn default() -> Self {
                Self {
                    $(
                        $setting: $crate::settings!(__or_default $($default)?),
                    )+
                }
            }
        }
    };
    (__or_default $default: expr) => {
        $default
    };
    (__or_default) => {
        ::std::default::Default::default()
    };
}

pub trait SettingsFromArgs: Default + clap::FromArgMatches + clap::Args {}

impl<T> SettingsFromArgs for T where T: Default + clap::FromArgMatches + clap::Args {}

/// Application settings.
///
/// Allows for easily pass multiple configurations values around.
struct Settings {
    cmdline_matches: clap::ArgMatches,
}

impl Settings {
    fn builder(cmd_descriptor: CmdDescriptor) -> SettingsBuilder {
        SettingsBuilder {
            cmd: Command::new(cmd_descriptor.name).about(cmd_descriptor.about),
            descriptor: cmd_descriptor,
        }
    }

    fn get<T: SettingsFromArgs>(&self) -> T {
        let mut settings = T::default();

        if let Err(err) = settings.update_from_arg_matches(&self.cmdline_matches) {
            err.print().expect("failed to write Settings parsing error");
            std::panic::panic_any(err);
        };

        settings
    }
}

/// A builder for [`Settings`].
pub struct SettingsBuilder {
    cmd: Command,
    descriptor: CmdDescriptor,
}

impl SettingsBuilder {
    /// Register the arguments for `A` in the command line parser.
    ///
    /// This will allow one to call [`Settings::get`] with `A` later on.
    pub fn register<A: SettingsFromArgs>(mut self) -> Self {
        self.cmd = A::augment_args(self.cmd);
        self
    }

    /// Build a [`Settings`] with the configured parser from the command line
    /// arguments given to the executable.
    pub fn install_from_args(mut self) {
        self = self
            .register::<crate::clients::HttpsClientSettings>()
            .register::<crate::server::ServerSettings>()
            .register::<crate::server::GrpcServiceSettings>()
            .register::<crate::clients::GrpcChannelSettings>()
            .register::<crate::observability::ObservabilitySettings>();

        self.cmd = self
            .cmd
            .name(self.descriptor.name)
            .about(self.descriptor.about);

        self.cmd.build();

        let settings = Settings {
            cmdline_matches: self.cmd.get_matches(),
        };

        if SETTINGS.set(settings).is_err() {
            panic!("Settings installed twice!");
        }
    }
}

/// clap value parser for [`Duration`] values.
#[derive(Debug, Default, Clone, Copy)]
pub struct DurationParser;

impl TypedValueParser for DurationParser {
    type Value = Duration;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &std::ffi::OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let int_parser = clap::value_parser!(u64);
        for (suffix, builder) in [
            (b"ns" as &[u8], Duration::from_nanos as fn(u64) -> Duration),
            (b"us", Duration::from_micros),
            (b"ms", Duration::from_millis),
            (b"s", Duration::from_secs),
        ] {
            if let Some(bytes) = value.as_encoded_bytes().strip_suffix(suffix) {
                let val_bytes = unsafe { OsStr::from_encoded_bytes_unchecked(bytes) };
                let value = int_parser.parse_ref(cmd, arg, val_bytes)?;
                return Ok(builder(value));
            }
        }

        let mut err = clap::Error::new(clap::error::ErrorKind::ValueValidation).with_cmd(cmd);
        err.insert(
            clap::error::ContextKind::InvalidValue,
            clap::error::ContextValue::String(
                "duration values must end in ns, us, ms or s.".into(),
            ),
        );

        Err(err)
    }
}

/// clap value parser for key values pairs.
#[derive(Debug, Default, Clone, Copy)]
pub struct KeyValueParser<T> {
    _phantom: PhantomData<T>,
}

impl<T: TypedValueParser + Default> TypedValueParser for KeyValueParser<T> {
    type Value = (String, T::Value);

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let mut splits = value.as_encoded_bytes().split(|b| *b == b'=');
        let Some((name, dur)) = splits.next().zip(splits.next()) else {
            let mut err = clap::Error::new(clap::error::ErrorKind::ValueValidation).with_cmd(cmd);
            if let Some(arg) = arg.and_then(|a| a.get_long()) {
                err.insert(
                    clap::error::ContextKind::InvalidValue,
                    clap::error::ContextValue::String(format!(
                        "argument {arg} value should be a pair <key>=<value>"
                    )),
                );
            }

            return Err(err);
        };

        if splits.next().is_some() {
            if let Some(arg) = arg.and_then(|a| a.get_long()) {
                let mut err =
                    clap::Error::new(clap::error::ErrorKind::ValueValidation).with_cmd(cmd);
                err.insert(
                    clap::error::ContextKind::InvalidValue,
                    clap::error::ContextValue::String(format!(
                        "argument {arg} value should be a pair <key>=<value>"
                    )),
                );
                return Err(err);
            }
        }

        let dur = T::default().parse_ref(cmd, arg, unsafe {
            OsStr::from_encoded_bytes_unchecked(dur)
        })?;

        Ok((String::from_utf8_lossy(name).into_owned(), dur))
    }
}

/// Parser for sizes prefixed up to Mi/M.
#[derive(Debug, Default, Clone, Copy)]
pub struct SizeParser;

impl TypedValueParser for SizeParser {
    type Value = usize;

    fn parse_ref(
        &self,
        cmd: &clap::Command,
        arg: Option<&clap::Arg>,
        value: &OsStr,
    ) -> Result<Self::Value, clap::Error> {
        let int_parser = clap::value_parser!(u32);
        for (suffix, multiplier) in [
            (b"Mi" as &[u8], 1024 * 1024),
            (b"M", 1000 * 1000),
            (b"Ki", 1024),
            (b"K", 1000),
        ] {
            if let Some(bytes) = value.as_encoded_bytes().strip_suffix(suffix) {
                let val_bytes = unsafe { OsStr::from_encoded_bytes_unchecked(bytes) };
                let value = int_parser.parse_ref(cmd, arg, val_bytes)?;
                return Ok(value as usize * multiplier);
            }
        }

        let mut err = clap::Error::new(clap::error::ErrorKind::ValueValidation).with_cmd(cmd);
        err.insert(
            clap::error::ContextKind::InvalidValue,
            clap::error::ContextValue::String("duration values must end in Mi, M, Ki or K.".into()),
        );

        Err(err)
    }
}
