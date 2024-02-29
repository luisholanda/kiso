use std::{borrow::Cow, time::SystemTime};

use opentelemetry::{
    trace::{Link, SpanContext, SpanId, SpanKind, Status, TraceFlags, TraceState},
    Key, KeyValue, Value,
};
use opentelemetry_sdk::trace::{IdGenerator, RandomIdGenerator};

use super::worker::{Command, LightSpanCtx};

/// A started span inside a trace.
#[derive(Clone)]
pub struct Span {
    ctx: SpanContext,
}

crate::impl_context_for_type!(Span);

impl Span {
    /// Starts the construction of a root span.
    pub fn root(name: impl Into<Cow<'static, str>>, kind: SpanKind) -> SpanBuilder {
        let id_gen = RandomIdGenerator::default();

        SpanBuilder {
            span_context: SpanContext::new(
                id_gen.new_trace_id(),
                id_gen.new_span_id(),
                // TODO: think in how to use otel-sdk's ShouldSample here.
                TraceFlags::SAMPLED,
                // These values makes sense as this is a root span.
                false,
                TraceState::default(),
            ),
            name: name.into(),
            kind,
            parent_id: SpanId::INVALID,
            attributes: Vec::with_capacity(8),
        }
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

    fn light_id(&self) -> LightSpanCtx {
        (self.ctx.trace_id(), self.ctx.span_id())
    }
}

pub struct SpanBuilder {
    pub(super) span_context: SpanContext,
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
            ctx: self.span_context,
        }
    }
}

pub(super) struct NewSpan {
    pub(super) span_context: SpanContext,
    pub(super) name: Cow<'static, str>,
    pub(super) kind: SpanKind,
    pub(super) parent_id: SpanId,
    pub(super) start_time: SystemTime,
    pub(super) attributes: Vec<KeyValue>,
}
