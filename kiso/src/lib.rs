#[macro_export]
macro_rules! declare_span_fields_struct {
    ($vis: vis $ty: ident { $($field: ident : $field_name: expr,)+ }) => {
        $vis struct $ty {
            $($vis $field: tracing::field::Field,)+
        }

        impl $ty {
            $vis fn instance(span: &tracing::Span) -> &'static Self {
                static INSTANCE: std::sync::OnceLock<$ty> = std::sync::OnceLock::new();

                INSTANCE.get_or_init(|| {
                    Self {
                        $($field: span.field($field_name).unwrap_or_else(|| panic!("field missing: {}", $field_name)),)+
                    }
                })
            }
        }
    };
}

pub mod clients;
pub mod context;
pub mod rt;

pub mod server;
/// Application settings.
pub mod settings;

pub mod tracing;

pub use self::rt::spawn;
