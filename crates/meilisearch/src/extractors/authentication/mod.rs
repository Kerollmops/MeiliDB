mod error;

use std::marker::PhantomData;
use std::ops::Deref;
use std::pin::Pin;

use actix_web::web::Data;
use actix_web::FromRequest;
pub use error::AuthenticationError;
use futures::future::err;
use futures::Future;
use meilisearch_auth::{AuthController, AuthFilter};
use meilisearch_types::error::{Code, ResponseError};

use self::policies::AuthError;

pub struct GuardedData<P, D> {
    data: D,
    filters: AuthFilter,
    _marker: PhantomData<P>,
}

impl<P, D> GuardedData<P, D> {
    pub fn filters(&self) -> &AuthFilter {
        &self.filters
    }

    async fn auth_bearer(
        auth: Data<AuthController>,
        token: String,
        index: Option<String>,
        data: Option<D>,
    ) -> Result<Self, ResponseError>
    where
        P: Policy + 'static,
    {
        let missing_master_key = auth.get_master_key().is_none();

        match Self::authenticate(auth, token, index).await? {
            Ok(filters) => match data {
                Some(data) => Ok(Self { data, filters, _marker: PhantomData }),
                None => Err(AuthenticationError::IrretrievableState.into()),
            },
            Err(_) if missing_master_key => Err(AuthenticationError::MissingMasterKey.into()),
            Err(e) => Err(ResponseError::from_msg(e.to_string(), Code::InvalidApiKey)),
        }
    }

    async fn auth_token(auth: Data<AuthController>, data: Option<D>) -> Result<Self, ResponseError>
    where
        P: Policy + 'static,
    {
        let missing_master_key = auth.get_master_key().is_none();

        match Self::authenticate(auth, String::new(), None).await? {
            Ok(filters) => match data {
                Some(data) => Ok(Self { data, filters, _marker: PhantomData }),
                None => Err(AuthenticationError::IrretrievableState.into()),
            },
            Err(_) if missing_master_key => Err(AuthenticationError::MissingMasterKey.into()),
            Err(_) => Err(AuthenticationError::MissingAuthorizationHeader.into()),
        }
    }

    async fn authenticate(
        auth: Data<AuthController>,
        token: String,
        index: Option<String>,
    ) -> Result<Result<AuthFilter, AuthError>, ResponseError>
    where
        P: Policy + 'static,
    {
        tokio::task::spawn_blocking(move || P::authenticate(auth, token.as_ref(), index.as_deref()))
            .await
            .map_err(|e| ResponseError::from_msg(e.to_string(), Code::Internal))
    }
}

impl<P, D> Deref for GuardedData<P, D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<P: Policy + 'static, D: 'static + Clone> FromRequest for GuardedData<P, D> {
    type Error = ResponseError;

    type Future = Pin<Box<dyn Future<Output = Result<Self, Self::Error>>>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        _payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        match req.app_data::<Data<AuthController>>().cloned() {
            Some(auth) => match req
                .headers()
                .get("Authorization")
                .map(|type_token| type_token.to_str().unwrap_or_default().splitn(2, ' '))
            {
                Some(mut type_token) => match type_token.next() {
                    Some("Bearer") => {
                        // TODO: find a less hardcoded way?
                        let index = req.match_info().get("index_uid");
                        match type_token.next() {
                            Some(token) => Box::pin(Self::auth_bearer(
                                auth,
                                token.to_string(),
                                index.map(String::from),
                                req.app_data::<D>().cloned(),
                            )),
                            None => Box::pin(err(AuthenticationError::InvalidToken.into())),
                        }
                    }
                    _otherwise => {
                        Box::pin(err(AuthenticationError::MissingAuthorizationHeader.into()))
                    }
                },
                None => Box::pin(Self::auth_token(auth, req.app_data::<D>().cloned())),
            },
            None => Box::pin(err(AuthenticationError::IrretrievableState.into())),
        }
    }
}

pub trait Policy {
    fn authenticate(
        auth: Data<AuthController>,
        token: &str,
        index: Option<&str>,
    ) -> Result<AuthFilter, policies::AuthError>;
}

pub mod policies {
    use actix_web::web::Data;
    use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
    use meilisearch_auth::{AuthController, AuthFilter, SearchRules};
    use meilisearch_types::error::{Code, ErrorCode};
    // reexport actions in policies in order to be used in routes configuration.
    pub use meilisearch_types::keys::{actions, Action};
    use serde::{Deserialize, Serialize};
    use time::OffsetDateTime;
    use uuid::Uuid;

    use crate::extractors::authentication::Policy;

    enum TenantTokenOutcome {
        NotATenantToken,
        Valid(Uuid, SearchRules),
    }

    #[derive(thiserror::Error, Debug)]
    pub enum AuthError {
        #[error("Tenant token expired. Was valid up to `{exp}` and we're now `{now}`.")]
        ExpiredTenantToken { exp: i64, now: i64 },
        #[error("The provided API key is invalid.")]
        InvalidApiKey,
        #[error("The provided tenant token cannot acces the index `{index}`, allowed indexes are {allowed:?}.")]
        TenantTokenAccessingnUnauthorizedIndex { index: String, allowed: Vec<String> },
        #[error(
            "The API key used to generate this tenant token cannot acces the index `{index}`."
        )]
        TenantTokenApiKeyAccessingnUnauthorizedIndex { index: String },
        #[error(
            "The API key cannot acces the index `{index}`, authorized indexes are {allowed:?}."
        )]
        ApiKeyAccessingnUnauthorizedIndex { index: String, allowed: Vec<String> },
        #[error("The provided tenant token is invalid.")]
        InvalidTenantToken,
        #[error("Could not decode tenant token, {0}.")]
        CouldNotDecodeTenantToken(jsonwebtoken::errors::Error),
        #[error("Invalid action `{0}`.")]
        InternalInvalidAction(u32),
    }

    impl From<jsonwebtoken::errors::Error> for AuthError {
        fn from(error: jsonwebtoken::errors::Error) -> Self {
            use jsonwebtoken::errors::ErrorKind;

            match error.kind() {
                ErrorKind::InvalidToken => AuthError::InvalidTenantToken,
                _ => AuthError::CouldNotDecodeTenantToken(error),
            }
        }
    }

    impl ErrorCode for AuthError {
        fn error_code(&self) -> Code {
            match self {
                AuthError::InternalInvalidAction(_) => Code::Internal,
                _ => Code::InvalidApiKey,
            }
        }
    }

    fn tenant_token_validation() -> Validation {
        let mut validation = Validation::default();
        validation.validate_exp = false;
        validation.required_spec_claims.remove("exp");
        validation.algorithms = vec![Algorithm::HS256, Algorithm::HS384, Algorithm::HS512];
        validation
    }

    /// Extracts the key id used to sign the payload, without performing any validation.
    fn extract_key_id(token: &str) -> Result<Uuid, AuthError> {
        let mut validation = tenant_token_validation();
        validation.insecure_disable_signature_validation();
        let dummy_key = DecodingKey::from_secret(b"secret");
        let token_data = decode::<Claims>(token, &dummy_key, &validation)?;

        // get token fields without validating it.
        let Claims { api_key_uid, .. } = token_data.claims;
        Ok(api_key_uid)
    }

    fn is_keys_action(action: u32) -> bool {
        use actions::*;
        matches!(action, KEYS_GET | KEYS_CREATE | KEYS_UPDATE | KEYS_DELETE)
    }

    pub struct ActionPolicy<const A: u32>;

    impl<const A: u32> Policy for ActionPolicy<A> {
        /// Attempts to grant authentication from a bearer token (that can be a tenant token or an API key), the requested Action,
        /// and a list of requested indexes.
        ///
        /// If the bearer token is not allowed for the specified indexes and action, returns `None`.
        /// Otherwise, returns an object containing the generated permissions: the search filters to add to a search, and the list of allowed indexes
        /// (that may contain more indexes than requested).
        fn authenticate(
            auth: Data<AuthController>,
            token: &str,
            index: Option<&str>,
        ) -> Result<AuthFilter, AuthError> {
            // authenticate if token is the master key.
            // Without a master key, all routes are accessible except the key-related routes.
            if auth.get_master_key().map_or_else(|| !is_keys_action(A), |mk| mk == token) {
                return Ok(AuthFilter::default());
            }

            let (key_uuid, search_rules) =
                match ActionPolicy::<A>::authenticate_tenant_token(&auth, token) {
                    Ok(TenantTokenOutcome::Valid(key_uuid, search_rules)) => {
                        (key_uuid, Some(search_rules))
                    }
                    Ok(TenantTokenOutcome::NotATenantToken)
                    | Err(AuthError::InvalidTenantToken) => (
                        auth.get_optional_uid_from_encoded_key(token.as_bytes())
                            .map_err(|_e| AuthError::InvalidApiKey)?
                            .ok_or(AuthError::InvalidApiKey)?,
                        None,
                    ),
                    Err(e) => return Err(e),
                };

            // check that the indexes are allowed
            let action = Action::from_bits(A).ok_or(AuthError::InternalInvalidAction(A))?;
            let auth_filter = auth
                .get_key_filters(key_uuid, search_rules)
                .map_err(|_e| AuthError::InvalidApiKey)?;

            // First check if the index is authorized in the tenant token, this is a public
            // information, we can return a nice error message.
            if let Some(index) = index {
                if !auth_filter.tenant_token_is_index_authorized(index) {
                    return Err(AuthError::TenantTokenAccessingnUnauthorizedIndex {
                        index: index.to_string(),
                        allowed: auth_filter.tenant_token_list_index_authorized(),
                    });
                }
                if !auth_filter.api_key_is_index_authorized(index) {
                    if auth_filter.is_tenant_token() {
                        // If the error comes from a tenant token we cannot share the list
                        // of authorized indexes in the API key. This is not public information.
                        return Err(AuthError::TenantTokenApiKeyAccessingnUnauthorizedIndex {
                            index: index.to_string(),
                        });
                    } else {
                        // Otherwise we can share the list
                        // of authorized indexes in the API key.
                        return Err(AuthError::ApiKeyAccessingnUnauthorizedIndex {
                            index: index.to_string(),
                            allowed: auth_filter.api_key_list_index_authorized(),
                        });
                    }
                }
            }
            if auth.is_key_authorized(key_uuid, action, index).unwrap_or(false) {
                return Ok(auth_filter);
            }

            Err(AuthError::InvalidApiKey)
        }
    }

    impl<const A: u32> ActionPolicy<A> {
        fn authenticate_tenant_token(
            auth: &AuthController,
            token: &str,
        ) -> Result<TenantTokenOutcome, AuthError> {
            // Only search action can be accessed by a tenant token.
            if A != actions::SEARCH {
                return Ok(TenantTokenOutcome::NotATenantToken);
            }

            let uid = extract_key_id(token)?;

            // Check if tenant token is valid.
            let key = if let Some(key) = auth.generate_key(uid) {
                key
            } else {
                return Err(AuthError::InvalidTenantToken);
            };

            let data = decode::<Claims>(
                token,
                &DecodingKey::from_secret(key.as_bytes()),
                &tenant_token_validation(),
            )?;

            // Check if token is expired.
            if let Some(exp) = data.claims.exp {
                let now = OffsetDateTime::now_utc().unix_timestamp();
                if now > exp {
                    return Err(AuthError::ExpiredTenantToken { exp, now });
                }
            }

            Ok(TenantTokenOutcome::Valid(uid, data.claims.search_rules))
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Claims {
        search_rules: SearchRules,
        exp: Option<i64>,
        api_key_uid: Uuid,
    }
}
