//! What one request declared about the client that sent it.
//!
//! MCP 2026-07-28 has no handshake to remember: there is no `initialize`, no
//! session, and therefore no negotiated capability set the server may carry
//! forward. Each request restates its own protocol version and client
//! capabilities in `params._meta`, and the server decides how to shape *that*
//! response from *those* declarations. [`RequestProfile`] is that reading, taken
//! once per request.
//!
//! Two constraints shape this module.
//!
//! The first is a correctness trap in the SDK. Inbound `_meta` is hoisted out of
//! the typed parameter structs during deserialization and delivered on
//! [`RequestContext::meta`], so the `meta` field of a `CallToolRequestParams`
//! (or any other params type) is **always `None`** server-side. Reading it
//! silently sees an empty declaration on every real request, which fails in the
//! most expensive direction: features look unsupported and stay off. Everything
//! here reads [`RequestContext::meta`] instead, and the parsing is factored so
//! it can be unit-tested against a [`RequestMetaObject`] without a live peer.
//!
//! The second is that declarations are self-reported. They say what a client can
//! *handle*, never who it *is*: capability text must never reach an
//! authorization decision. See [`crate::mcp::authorization`] for identity.

use rmcp::{
    RoleServer,
    model::{ClientCapabilities, ProgressToken, ProtocolVersion, RequestMetaObject},
    service::RequestContext,
};

/// The capability picture one request declared for itself.
#[derive(Clone, Debug, Default)]
pub struct RequestProfile {
    protocol_version: Option<ProtocolVersion>,
    capabilities: Option<ClientCapabilities>,
    progress_token: Option<ProgressToken>,
    required_metadata_present: bool,
}

impl RequestProfile {
    /// Read the profile of the request being handled.
    pub fn from_context(context: &RequestContext<RoleServer>) -> Self {
        Self::from_meta(&context.meta)
    }

    /// Read a profile from request metadata alone.
    ///
    /// This is the whole parsing seam: the context adds nothing but the map.
    pub fn from_meta(meta: &RequestMetaObject) -> Self {
        Self {
            protocol_version: meta.protocol_version(),
            capabilities: meta.client_capabilities(),
            progress_token: meta.get_progress_token(),
            required_metadata_present: meta
                .missing_required_keys(&ProtocolVersion::V_2026_07_28)
                .is_empty(),
        }
    }

    /// Protocol version this request declared, if it declared one.
    pub fn protocol_version(&self) -> Option<&ProtocolVersion> {
        self.protocol_version.as_ref()
    }

    /// Whether the request carried every `_meta` field 2026-07-28 requires.
    ///
    /// False means the request is not self-describing. The transport and the
    /// SDK already reject those before dispatch, so a handler seeing `false`
    /// is looking at a legacy or hand-rolled caller and should not infer
    /// support for anything.
    pub fn declares_required_metadata(&self) -> bool {
        self.required_metadata_present
    }

    /// Whether the request declared the given MCP extension.
    ///
    /// Extensions ride in `clientCapabilities.extensions` as a map of
    /// extension id to a settings object, so presence of the key is the
    /// declaration and an empty object means "supported, no settings".
    #[allow(
        dead_code,
        reason = "the per-request tasks-extension gate that reads this lands separately"
    )]
    pub fn declares_extension(&self, id: &str) -> bool {
        self.capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.extensions.as_ref())
            .is_some_and(|extensions| extensions.contains_key(id))
    }

    /// Extension ids this request declared, in a stable order.
    pub fn extension_ids(&self) -> Vec<&str> {
        self.capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.extensions.as_ref())
            .map(|extensions| extensions.keys().map(String::as_str).collect())
            .unwrap_or_default()
    }

    /// Whether the client can answer a form-mode elicitation.
    ///
    /// An `elicitation` object with no modes selected means form mode, which is
    /// the original and only universally supported mode; a client that declared
    /// `url` alone has opted into URL mode *instead*, and must not be sent a
    /// form.
    pub fn supports_form_elicitation(&self) -> bool {
        self.capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.elicitation.as_ref())
            .is_some_and(|elicitation| elicitation.form.is_some() || elicitation.url.is_none())
    }

    /// Progress token this request opted in with, if any.
    ///
    /// Progress notifications may only reference a token an active request
    /// supplied, so the absence of one means the caller asked for silence.
    pub fn progress_token(&self) -> Option<&ProgressToken> {
        self.progress_token.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use rmcp::model::{ElicitationCapability, FormElicitationCapability, UrlElicitationCapability};

    use super::*;

    /// Build request metadata the way a well-formed 2026-07-28 client would.
    fn meta(capabilities: ClientCapabilities) -> RequestMetaObject {
        let mut meta = RequestMetaObject::new();
        meta.set_protocol_version(ProtocolVersion::V_2026_07_28);
        meta.set_client_capabilities(capabilities);
        meta
    }

    #[test]
    fn a_request_without_metadata_declares_nothing() {
        let profile = RequestProfile::from_meta(&RequestMetaObject::new());
        assert!(!profile.declares_required_metadata());
        assert_eq!(profile.protocol_version(), None);
        assert!(profile.extension_ids().is_empty());
        assert!(!profile.declares_extension(rmcp::model::TASKS_EXTENSION_ID));
        assert!(!profile.supports_form_elicitation());
        assert!(profile.progress_token().is_none());
    }

    #[test]
    fn a_self_describing_request_reports_its_declared_version() {
        let profile = RequestProfile::from_meta(&meta(ClientCapabilities::default()));
        assert!(profile.declares_required_metadata());
        assert_eq!(
            profile.protocol_version(),
            Some(&ProtocolVersion::V_2026_07_28)
        );
    }

    #[test]
    fn declaring_the_protocol_version_alone_is_not_enough() {
        // clientCapabilities is required too: without it the server would have
        // to guess what the caller can follow.
        let mut partial = RequestMetaObject::new();
        partial.set_protocol_version(ProtocolVersion::V_2026_07_28);
        assert!(!RequestProfile::from_meta(&partial).declares_required_metadata());
    }

    #[test]
    fn an_extension_is_declared_by_the_presence_of_its_id() {
        let capabilities = ClientCapabilities::builder().enable_tasks().build();
        let profile = RequestProfile::from_meta(&meta(capabilities));
        assert!(profile.declares_extension(rmcp::model::TASKS_EXTENSION_ID));
        assert!(!profile.declares_extension("io.modelcontextprotocol/ui"));
        assert_eq!(
            profile.extension_ids(),
            vec![rmcp::model::TASKS_EXTENSION_ID]
        );
    }

    #[test]
    fn an_empty_elicitation_capability_means_form_mode() {
        let mut capabilities = ClientCapabilities::default();
        capabilities.elicitation = Some(ElicitationCapability::new());
        assert!(RequestProfile::from_meta(&meta(capabilities)).supports_form_elicitation());
    }

    #[test]
    fn an_explicit_form_capability_means_form_mode() {
        let mut capabilities = ClientCapabilities::default();
        capabilities.elicitation =
            Some(ElicitationCapability::new().with_form(FormElicitationCapability::new()));
        assert!(RequestProfile::from_meta(&meta(capabilities)).supports_form_elicitation());
    }

    #[test]
    fn a_url_only_client_must_not_be_sent_a_form() {
        let mut capabilities = ClientCapabilities::default();
        capabilities.elicitation =
            Some(ElicitationCapability::new().with_url(UrlElicitationCapability::new()));
        assert!(!RequestProfile::from_meta(&meta(capabilities)).supports_form_elicitation());
    }

    #[test]
    fn a_client_that_declared_no_elicitation_gets_no_input_requests() {
        assert!(
            !RequestProfile::from_meta(&meta(ClientCapabilities::default()))
                .supports_form_elicitation()
        );
    }

    #[test]
    fn progress_is_opt_in_per_request() {
        let mut with_token = meta(ClientCapabilities::default());
        with_token.set_progress_token(ProgressToken(rmcp::model::NumberOrString::String(
            "transfer-1".into(),
        )));
        assert_eq!(
            RequestProfile::from_meta(&with_token)
                .progress_token()
                .map(|token| token.0.to_string()),
            Some("transfer-1".to_string())
        );
        assert!(
            RequestProfile::from_meta(&meta(ClientCapabilities::default()))
                .progress_token()
                .is_none()
        );
    }
}
