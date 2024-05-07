use std::{borrow::Cow, future::Future, str::FromStr, time::SystemTime};

pub use opentelemetry::trace::{Link, SpanKind, Status};
use opentelemetry::{
    trace::{SpanContext, SpanId, TraceFlags, TraceId, TraceState},
    Key, KeyValue, Value,
};
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator};

use super::worker::{Command, LightSpanCtx};

/// A started span inside a trace.
pub struct Span {
    end_on_drop: bool,
    ctx: Box<SpanContext>,
}

crate::impl_context_for_type!(Span);

impl Span {
    /// Starts the construction of a new span.
    pub fn builder(name: impl Into<Cow<'static, str>>, kind: SpanKind) -> SpanBuilder {
        let id_gen = RandomIdGenerator::default();

        let (span_ctx, parent_id) = crate::context::try_with::<Span, _>(|parent| {
            let ctx = Box::new(SpanContext::new(
                parent.ctx.trace_id(),
                id_gen.new_span_id(),
                parent.ctx.trace_flags(),
                false,
                parent.ctx.trace_state().clone(),
            ));

            (ctx, parent.ctx.span_id())
        })
        .unwrap_or_else(|| {
            let ctx = Box::new(SpanContext::new(
                id_gen.new_trace_id(),
                id_gen.new_span_id(),
                // TODO: think in how to use otel-sdk's ShouldSample here.
                TraceFlags::SAMPLED,
                // These values makes sense as this is a root span.
                false,
                TraceState::default(),
            ));

            (ctx, SpanId::INVALID)
        });

        SpanBuilder {
            span_context: span_ctx,
            name: name.into(),
            kind,
            parent_id,
            attributes: Vec::with_capacity(8),
        }
    }

    /// Starts the construction of a new internal span.
    ///
    /// This is how most spans in an application should be created, as the other
    /// kinds of span are mostly handled by libries.
    pub fn internal(name: impl Into<Cow<'static, str>>) -> SpanBuilder {
        Self::builder(name, SpanKind::Internal)
    }

    /// The OpenTelemetry span context of this span.
    pub fn span_context(&self) -> &SpanContext {
        &self.ctx
    }

    /// Set an attribute in the span after it is started.
    ///
    /// Setting an attribute with the same key as an existing attribute results in both
    /// being stored as attribute, without any de-duplication performed.
    ///
    /// Note that the OpenTelemetry project documents certain “standard attributes” that
    /// have prescribed semantic meanings and are available via the
    /// [`opentelemetry_semantic_conventions`] crate.
    pub fn set_attribute<K, V>(&self, key: K, value: V)
    where
        K: Into<Key>,
        V: Into<Value>,
    {
        super::send_cmd(Command::SetSpanAttr(
            self.light_id(),
            KeyValue::new(key, value),
        ));
    }

    /// Sets the status of this Span.
    ///
    /// If used, this will override the default span status, which is Status::Unset.
    pub fn set_status(&self, status: Status) {
        super::send_cmd(Command::SetSpanStatus(self.light_id(), status));
    }

    /// Update the span's name.
    pub fn update_name(&self, new_name: impl Into<Cow<'static, str>>) {
        super::send_cmd(Command::UpdateSpanName(self.light_id(), new_name.into()));
    }

    /// Add a link to this span.
    ///
    /// A link marks a relationship between this span and another one.
    pub fn add_link(&self, link: Link) {
        super::send_cmd(Command::AddLinkToSpan(self.light_id(), link));
    }

    /// Encode this span as a W3C trace parent header value.
    pub fn to_w3c_traceparent(&self) -> String {
        format!(
            "00-{}-{}-{:02x}",
            self.ctx.trace_id(),
            self.ctx.span_id(),
            self.ctx.trace_flags() & TraceFlags::SAMPLED
        )
    }

    fn light_id(&self) -> LightSpanCtx {
        (self.ctx.trace_id(), self.ctx.span_id())
    }
}

impl Clone for Span {
    fn clone(&self) -> Self {
        Self {
            end_on_drop: false,
            ctx: self.ctx.clone(),
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if self.end_on_drop {
            super::send_cmd(Command::EndSpan(self.light_id(), SystemTime::now()));
        }
    }
}

pub struct SpanBuilder {
    pub(super) span_context: Box<SpanContext>,
    pub(super) name: Cow<'static, str>,
    pub(super) kind: SpanKind,
    pub(super) parent_id: SpanId,
    pub(super) attributes: Vec<KeyValue>,
}

impl SpanBuilder {
    /// Add an attribute to the span.
    pub fn attr<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<Key>,
        V: Into<Value>,
    {
        self.attributes.push(KeyValue::new(key, value));
        self
    }

    /// Change the name of the span, before starting it.
    pub fn set_name(mut self, new_name: impl Into<Cow<'static, str>>) -> Self {
        self.name = new_name.into();
        self
    }

    /// Start the span.
    pub fn start(self) -> Span {
        super::send_cmd(Command::NewSpan(NewSpan {
            span_context: self.span_context.clone(),
            name: self.name,
            kind: self.kind,
            parent_id: self.parent_id,
            start_time: SystemTime::now(),
            attributes: self.attributes,
        }));

        Span {
            end_on_drop: true,
            ctx: self.span_context,
        }
    }

    /// Set the span parent from the given W3C trace headers.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the `traceparent` header is invalid.
    pub fn with_w3c_parent(
        mut self,
        traceparent: &str,
        tracestate: Option<&str>,
    ) -> Result<Self, Self> {
        fn parse_ctx(
            traceparent: &str,
            tracestate: Option<&str>,
        ) -> Option<(Box<SpanContext>, SpanId)> {
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

            let tracestate = tracestate
                .and_then(|s| TraceState::from_str(s).ok())
                .unwrap_or_default();

            if parts.next().is_some() {
                return None;
            }

            let ctx = Box::new(SpanContext::new(
                trace_id,
                RandomIdGenerator::default().new_span_id(),
                trace_flags,
                false,
                tracestate,
            ));

            Some((ctx, span_id))
        }

        let Some((ctx, span_id)) = parse_ctx(traceparent, tracestate) else {
            return Err(self);
        };

        if ctx.is_valid() {
            self.span_context = ctx;
            self.parent_id = span_id;

            Ok(self)
        } else {
            Err(self)
        }
    }
}

pub(super) struct NewSpan {
    pub(super) span_context: Box<SpanContext>,
    pub(super) name: Cow<'static, str>,
    pub(super) kind: SpanKind,
    pub(super) parent_id: SpanId,
    pub(super) start_time: SystemTime,
    pub(super) attributes: Vec<KeyValue>,
}

/// Helper trait for Future instrumentation.
pub trait Instrument: Sized + Future {
    /// Instrument the future with the given span.
    fn instrument(self, span: Span) -> impl Future<Output = Self::Output> + Send;

    /// Instrument the future with the current span.
    fn in_current_span(self) -> impl Future<Output = Self::Output> + Send;
}

impl<F: Future + Send> Instrument for F {
    #[inline(always)]
    fn in_current_span(self) -> impl Future<Output = Self::Output> + Send {
        self.instrument(Span::current())
    }

    #[inline(always)]
    fn instrument(self, span: Span) -> impl Future<Output = Self::Output> + Send {
        crate::context::scope(span, self)
    }
}
