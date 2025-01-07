use actix_web::http::StatusCode;
use actix_web::test;
use jsonwebtoken::{EncodingKey, Header};
use meili_snap::*;
use uuid::Uuid;

use crate::common::{Server, Value};
use crate::json;

#[actix_rt::test]
async fn create_api_key_bad_description() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.add_api_key(json!({ "description": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.description`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_api_key_description",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_description"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_bad_name() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.add_api_key(json!({ "name": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.name`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_api_key_name",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_name"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_bad_uid() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // bad type
    let (response, code) = server.add_api_key(json!({ "uid": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.uid`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_api_key_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_uid"
    }
    "###);

    // can't parse
    let (response, code) = server.add_api_key(json!({ "uid": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.uid`: invalid character: expected an optional prefix of `urn:uuid:` followed by [0-9a-fA-F-], found `o` at 2",
      "code": "invalid_api_key_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_uid"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_bad_actions() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // bad type
    let (response, code) = server.add_api_key(json!({ "actions": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.actions`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_actions"
    }
    "###);

    // can't parse
    let (response, code) = server.add_api_key(json!({ "actions": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown value `doggo` at `.actions[0]`: expected one of `search`, `documents.add`, `documents.get`, `documents.delete`, `documents.*`, `indexes.create`, `indexes.get`, `indexes.update`, `indexes.delete`, `indexes.swap`, `indexes.*`, `tasks.cancel`, `tasks.delete`, `tasks.get`, `tasks.*`, `settings.get`, `settings.update`, `settings.*`, `stats.get`, `stats.*`, `metrics.get`, `metrics.*`, `dumps.create`, `dumps.*`, `snapshots.create`, `snapshots.*`, `version`, `keys.create`, `keys.get`, `keys.update`, `keys.delete`, `experimental.get`, `experimental.update`, `*`",
      "code": "invalid_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_actions"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_bad_indexes() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // bad type
    let (response, code) = server.add_api_key(json!({ "indexes": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.indexes`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_indexes"
    }
    "###);

    // can't parse
    let (response, code) = server.add_api_key(json!({ "indexes": ["good doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.indexes[0]`: `good doggo` is not a valid index uid pattern. Index uid patterns can be an integer or a string containing only alphanumeric characters, hyphens (-), underscores (_), and optionally end with a star (*).",
      "code": "invalid_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_indexes"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_bad_expires_at() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    // bad type
    let (response, code) = server.add_api_key(json!({ "expires_at": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `expires_at`: did you mean `expiresAt`? expected one of `description`, `name`, `uid`, `actions`, `indexes`, `expiresAt`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // can't parse
    let (response, code) = server.add_api_key(json!({ "expires_at": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `expires_at`: did you mean `expiresAt`? expected one of `description`, `name`, `uid`, `actions`, `indexes`, `expiresAt`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_missing_action() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) =
        server.add_api_key(json!({ "indexes": ["doggo"], "expiresAt": null })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `actions`",
      "code": "missing_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_api_key_actions"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_missing_indexes() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server
        .add_api_key(json!({ "uid": Uuid::nil() , "actions": ["*"], "expiresAt": null }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `indexes`",
      "code": "missing_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_api_key_indexes"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_missing_expires_at() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server
        .add_api_key(json!({ "uid": Uuid::nil(), "actions": ["*"], "indexes": ["doggo"] }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `expiresAt`",
      "code": "missing_api_key_expires_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_api_key_expires_at"
    }
    "###);
}

#[actix_rt::test]
async fn create_api_key_unexpected_field() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server
        .add_api_key(json!({ "uid": Uuid::nil(), "actions": ["*"], "indexes": ["doggo"], "expiresAt": null, "doggo": "bork" }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `doggo`: expected one of `description`, `name`, `uid`, `actions`, `indexes`, `expiresAt`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn list_api_keys_bad_offset() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.list_api_keys("?offset=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `doggo` as a positive integer",
      "code": "invalid_api_key_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_offset"
    }
    "###);
}

#[actix_rt::test]
async fn list_api_keys_bad_limit() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.list_api_keys("?limit=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `doggo` as a positive integer",
      "code": "invalid_api_key_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_limit"
    }
    "###);
}

#[actix_rt::test]
async fn list_api_keys_unexpected_field() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.list_api_keys("?doggo=no_limit").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown parameter `doggo`: expected one of `offset`, `limit`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_bad_description() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "description": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.description`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_api_key_description",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_description"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_bad_name() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "name": ["doggo"] })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.name`: expected a string, but found an array: `[\"doggo\"]`",
      "code": "invalid_api_key_name",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_name"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_uid() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "uid": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `uid`: expected one of `description`, `name`",
      "code": "immutable_api_key_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_uid"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_actions() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "actions": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `actions`: expected one of `description`, `name`",
      "code": "immutable_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_actions"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_indexes() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "indexes": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `indexes`: expected one of `description`, `name`",
      "code": "immutable_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_indexes"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_expires_at() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "expiresAt": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `expiresAt`: expected one of `description`, `name`",
      "code": "immutable_api_key_expires_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_expires_at"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_created_at() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "createdAt": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `createdAt`: expected one of `description`, `name`",
      "code": "immutable_api_key_created_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_created_at"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_immutable_updated_at() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "updatedAt": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Immutable field `updatedAt`: expected one of `description`, `name`",
      "code": "immutable_api_key_updated_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_updated_at"
    }
    "###);
}

#[actix_rt::test]
async fn patch_api_keys_unknown_field() {
    let mut server = Server::new_auth().await;
    server.use_admin_key("MASTER_KEY").await;

    let (response, code) = server.patch_api_key("doggo", json!({ "doggo": "bork" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Unknown field `doggo`: expected one of `description`, `name`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

async fn send_request_with_custom_auth(
    app: impl actix_web::dev::Service<
        actix_http::Request,
        Response = actix_web::dev::ServiceResponse<impl actix_web::body::MessageBody>,
        Error = actix_web::Error,
    >,
    url: &str,
    auth: &str,
) -> (Value, StatusCode) {
    let req = test::TestRequest::get().uri(url).insert_header(("Authorization", auth)).to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();

    (response, status_code)
}

#[actix_rt::test]
async fn invalid_auth_format() {
    let server = Server::new_auth().await;
    let app = server.init_web_app().await;

    let req = test::TestRequest::get().uri("/indexes/dog/documents").to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"401 Unauthorized");
    snapshot!(response, @r###"
    {
      "message": "The Authorization header is missing. It must use the bearer authorization method.",
      "code": "missing_authorization_header",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    }
    "###);

    let req = test::TestRequest::get().uri("/indexes/dog/documents").to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"401 Unauthorized");
    snapshot!(response, @r###"
    {
      "message": "The Authorization header is missing. It must use the bearer authorization method.",
      "code": "missing_authorization_header",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    }
    "###);

    let (response, status_code) =
        send_request_with_custom_auth(&app, "/indexes/dog/documents", "Bearer").await;
    snapshot!(status_code, @"403 Forbidden");
    snapshot!(response, @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);
}

#[actix_rt::test]
async fn invalid_api_key() {
    let server = Server::new_auth().await;
    let app = server.init_web_app().await;

    let (response, status_code) =
        send_request_with_custom_auth(&app, "/indexes/dog/search", "Bearer kefir").await;
    snapshot!(status_code, @"403 Forbidden");
    snapshot!(response, @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);

    let uuid = Uuid::nil();
    let key = json!({ "actions": ["search"], "indexes": ["dog"], "expiresAt": null, "uid": uuid.to_string() });
    let req = test::TestRequest::post()
        .uri("/keys")
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .set_json(&key)
        .to_request();
    let res = test::call_service(&app, req).await;
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(json_string!(response, { ".createdAt" => "[date]",  ".updatedAt" => "[date]" }), @r###"
    {
      "name": null,
      "description": null,
      "key": "aeb94973e0b6e912d94165430bbe87dee91a7c4f891ce19050c3910ec96977e9",
      "uid": "00000000-0000-0000-0000-000000000000",
      "actions": [
        "search"
      ],
      "indexes": [
        "dog"
      ],
      "expiresAt": null,
      "createdAt": "[date]",
      "updatedAt": "[date]"
    }
    "###);
    let key = response["key"].as_str().unwrap();

    let (response, status_code) =
        send_request_with_custom_auth(&app, "/indexes/doggo/search", &format!("Bearer {key}"))
            .await;
    snapshot!(status_code, @"403 Forbidden");
    snapshot!(response, @r###"
    {
      "message": "The API key cannot acces the index `doggo`, authorized indexes are [\"dog\"].",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);
}

#[actix_rt::test]
async fn invalid_tenant_token() {
    let server = Server::new_auth().await;
    let app = server.init_web_app().await;

    // The tenant token won't be recognized at all if we're not on a search route
    let claims = json!({ "tamo": "kefir" });
    let jwt = jsonwebtoken::encode(&Header::default(), &claims, &EncodingKey::from_secret(b"tamo"))
        .unwrap();
    let (response, status_code) =
        send_request_with_custom_auth(&app, "/indexes/dog/documents", &format!("Bearer {jwt}"))
            .await;
    snapshot!(status_code, @"403 Forbidden");
    snapshot!(response, @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);

    let claims = json!({ "tamo": "kefir" });
    let jwt = jsonwebtoken::encode(&Header::default(), &claims, &EncodingKey::from_secret(b"tamo"))
        .unwrap();
    let (response, status_code) =
        send_request_with_custom_auth(&app, "/indexes/dog/search", &format!("Bearer {jwt}")).await;
    snapshot!(status_code, @"403 Forbidden");
    snapshot!(response, @r###"
    {
      "message": "Could not decode tenant token, JSON error: missing field `searchRules` at line 1 column 16.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);

    // The error messages are not ideal but that's expected since we cannot _yet_ use deserr
    let claims = json!({ "searchRules": "kefir" });
    let jwt = jsonwebtoken::encode(&Header::default(), &claims, &EncodingKey::from_secret(b"tamo"))
        .unwrap();
    let (response, status_code) =
        send_request_with_custom_auth(&app, "/indexes/dog/search", &format!("Bearer {jwt}")).await;
    snapshot!(status_code, @"403 Forbidden");
    snapshot!(response, @r###"
    {
      "message": "Could not decode tenant token, JSON error: data did not match any variant of untagged enum SearchRules at line 1 column 23.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);

    let uuid = Uuid::nil();
    let claims = json!({ "searchRules": ["kefir"], "apiKeyUid": uuid.to_string() });
    let jwt = jsonwebtoken::encode(&Header::default(), &claims, &EncodingKey::from_secret(b"tamo"))
        .unwrap();
    let (response, status_code) =
        send_request_with_custom_auth(&app, "/indexes/dog/search", &format!("Bearer {jwt}")).await;
    snapshot!(status_code, @"403 Forbidden");
    snapshot!(response, @r###"
    {
      "message": "Could not decode tenant token, InvalidSignature.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);

    // ~~ For the next tests we first need a valid API key
    let key = json!({ "actions": ["search"], "indexes": ["dog"], "expiresAt": null, "uid": uuid.to_string() });
    let req = test::TestRequest::post()
        .uri("/keys")
        .insert_header(("Authorization", "Bearer MASTER_KEY"))
        .set_json(&key)
        .to_request();
    let res = test::call_service(&app, req).await;
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(json_string!(response, { ".createdAt" => "[date]",  ".updatedAt" => "[date]" }), @r###"
    {
      "name": null,
      "description": null,
      "key": "aeb94973e0b6e912d94165430bbe87dee91a7c4f891ce19050c3910ec96977e9",
      "uid": "00000000-0000-0000-0000-000000000000",
      "actions": [
        "search"
      ],
      "indexes": [
        "dog"
      ],
      "expiresAt": null,
      "createdAt": "[date]",
      "updatedAt": "[date]"
    }
    "###);
    let key = response["key"].as_str().unwrap();

    let claims = json!({ "searchRules": ["doggo", "catto"], "apiKeyUid": uuid.to_string() });
    let jwt = jsonwebtoken::encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(key.as_bytes()),
    )
    .unwrap();
    // Try to access an index that is not authorized by the tenant token
    let (response, status_code) =
        send_request_with_custom_auth(&app, "/indexes/dog/search", &format!("Bearer {jwt}")).await;
    snapshot!(status_code, @"403 Forbidden");
    snapshot!(response, @r###"
    {
      "message": "The provided tenant token cannot acces the index `dog`, allowed indexes are [\"catto\", \"doggo\"].",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);

    // Try to access an index that *is* authorized by the tenant token but not by the api key used to generate the tt
    let (response, status_code) =
        send_request_with_custom_auth(&app, "/indexes/doggo/search", &format!("Bearer {jwt}"))
            .await;
    snapshot!(status_code, @"403 Forbidden");
    snapshot!(response, @r###"
    {
      "message": "The API key used to generate this tenant token cannot acces the index `doggo`.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);
}
