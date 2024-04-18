use std::{borrow::Cow, future::Future, sync::Arc};

use futures_util::future::BoxFuture;
use hyper::{
    header::{self, HeaderName, HeaderValue},
    http::request::Parts,
    Request,
};

/// A type that can authenticate requests.
pub trait Authenticator: Send + Sync + 'static {
    /// The error returned by this auhenticator when it can't ensure its
    /// internal state is ready.
    type Error;

    /// Ensure the authenticator can authenticate further requests.
    ///
    /// This will be called before each request, authenticators must cache
    /// authentication data and only reauthenticate when needed.
    fn ensure_authenticated(&self) -> impl Future<Output = Result<(), Self::Error>> + Send + '_;

    /// Add authentication data to the request.
    ///
    /// This receives the full request head as some authentication schemes use
    /// query string parameters.
    fn authenticate_request(&self, req: &mut Parts);
}

/// Skip the authentication procedure from [`Authenticated`] when added to
/// the extensions of a request.
pub struct NoAuthentication;

/// A wrapper service that authenticates requests before passing to the wrapped
/// service.
///
/// Use [`NoAuthentication`] to skip the authentication procedure.
pub struct Authenticated<S, A> {
    inner: S,
    authenticator: Arc<A>,
    ready_inner: Option<S>,
}

impl<S, A> Authenticated<S, A> {
    /// Constructs a new authenticated service.
    pub fn new(service: S, authenticator: Arc<A>) -> Self {
        Self {
            inner: service,
            authenticator,
            ready_inner: None,
        }
    }
}

impl<B, S, A> tower::Service<Request<B>> for Authenticated<S, A>
where
    A: Authenticator,
    S: tower::Service<Request<B>> + Clone + Send + 'static,
    S::Future: Send,
    S::Error: From<A::Error>,
    B: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<'static, Result<S::Response, S::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::ready!(self.inner.poll_ready(cx))?;

        let inner_clone = self.inner.clone();
        self.ready_inner = Some(std::mem::replace(&mut self.inner, inner_clone));

        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request<B>) -> Self::Future {
        let mut service = self.ready_inner.take().expect("poll_ready not called");
        let authenticator = self.authenticator.clone();

        Box::pin(async move {
            if req.extensions().get::<NoAuthentication>().is_none() {
                authenticator.ensure_authenticated().await?;

                let (mut parts, body) = req.into_parts();
                authenticator.authenticate_request(&mut parts);

                req = Request::from_parts(parts, body);
            }

            let fut = service.call(req);
            drop((authenticator, service));

            fut.await
        })
    }
}

/// An [`Authenticator`] that uses a fixed token.
pub struct FixedToken {
    token: HeaderValue,
    header: HeaderName,
}

impl FixedToken {
    /// Obtain the token from the given environment variable.
    ///
    /// Returns `None` if the environment variable isn't present or if its
    /// value is invalid.
    ///
    /// The environment variable is removed after this function is called.
    pub fn from_env_var(var: &'static str) -> Option<Self> {
        let val = std::env::var_os(var)?;
        std::env::remove_var(var);

        let mut token = HeaderValue::try_from(val.into_encoded_bytes()).ok()?;
        token.set_sensitive(true);

        Some(Self {
            token,
            header: header::AUTHORIZATION,
        })
    }

    /// Changes the header injected with the token in the request.
    ///
    /// Defaults to `Authorization`.
    pub fn with_header(mut self, header: impl Into<Cow<'static, str>>) -> Self {
        self.header = match header.into() {
            Cow::Borrowed(name) => HeaderName::from_static(name),
            Cow::Owned(name) => HeaderName::try_from(name).expect("invalid header name"),
        };
        self
    }
}

impl Authenticator for FixedToken {
    type Error = std::convert::Infallible;

    #[inline(always)]
    async fn ensure_authenticated(&self) -> Result<(), std::convert::Infallible> {
        Ok(())
    }

    #[inline(always)]
    fn authenticate_request(&self, req: &mut Parts) {
        req.headers.insert(self.header.clone(), self.token.clone());
    }
}
