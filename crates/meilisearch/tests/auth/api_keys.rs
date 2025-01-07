use std::{thread, time};

use crate::common::{Server, Value};
use crate::json;

#[actix_rt::test]
async fn add_valid_api_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "name": "indexing-key",
        "description": "Indexing API key",
        "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"201 Created");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "name": "indexing-key",
      "description": "Indexing API key",
      "key": "d9e776b8412f1db6974c9a5556b961c3559440b6588216f4ea5d9ed49f7c8f3c",
      "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "tasks.get",
        "settings.get",
        "settings.update",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
}

#[actix_rt::test]
async fn add_valid_api_key_expired_at() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"201 Created");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "Indexing API key",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "tasks.get",
        "settings.get",
        "settings.update",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
}

#[actix_rt::test]
async fn add_valid_api_key_no_description() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"201 Created");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": null,
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "documents.add"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
}

#[actix_rt::test]
async fn add_valid_api_key_null_description() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": json!(null),
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"201 Created");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": null,
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "documents.add"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_api_key_no_header() {
    let server = Server::new_auth().await;
    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"401 Unauthorized");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "The Authorization header is missing. It must use the bearer authorization method.",
      "code": "missing_authorization_header",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_api_key_bad_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("d4000bd7225f77d1eb22cc706ed36772bbc36767c016a27f76def7537b68600d");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"403 Forbidden");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_api_key_missing_parameter() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    // missing indexes
    let content = json!({
        "description": "Indexing API key",
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Missing field `indexes`",
      "code": "missing_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_api_key_indexes"
    }
    "###);

    // missing actions
    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Missing field `actions`",
      "code": "missing_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_api_key_actions"
    }
    "###);

    // missing expiration date
    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": ["documents.add"],
    });
    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "message": "Missing field `expiresAt`",
      "code": "missing_api_key_expires_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_api_key_expires_at"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_description() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": {"name":"products"},
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Invalid value type at `.description`: expected a string, but found an object: `{\"name\":\"products\"}`",
      "code": "invalid_api_key_description",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_description"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_name() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "name": {"name":"products"},
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Invalid value type at `.name`: expected a string, but found an object: `{\"name\":\"products\"}`",
      "code": "invalid_api_key_name",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_name"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_indexes() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": {"name":"products"},
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Invalid value type at `.indexes`: expected an array, but found an object: `{\"name\":\"products\"}`",
      "code": "invalid_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_indexes"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_index_uids() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": json!(null),
        "indexes": ["invalid index # / \\name with spaces"],
        "actions": [
            "documents.add"
        ],
        "expiresAt": "2050-11-13T00:00:00"
    });
    let (response, code) = server.add_api_key(content).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Invalid value at `.indexes[0]`: `invalid index # / \\name with spaces` is not a valid index uid pattern. Index uid patterns can be an integer or a string containing only alphanumeric characters, hyphens (-), underscores (_), and optionally end with a star (*).",
      "code": "invalid_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_indexes"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_actions() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": {"name":"products"},
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;
    assert_eq!(400, code, "{:?}", &response);

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Invalid value type at `.actions`: expected an array, but found an object: `{\"name\":\"products\"}`",
      "code": "invalid_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_actions"
    }
    "###);

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "doc.add"
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Unknown value `doc.add` at `.actions[0]`: expected one of `search`, `documents.add`, `documents.get`, `documents.delete`, `documents.*`, `indexes.create`, `indexes.get`, `indexes.update`, `indexes.delete`, `indexes.swap`, `indexes.*`, `tasks.cancel`, `tasks.delete`, `tasks.get`, `tasks.*`, `settings.get`, `settings.update`, `settings.*`, `stats.get`, `stats.*`, `metrics.get`, `metrics.*`, `dumps.create`, `dumps.*`, `snapshots.create`, `snapshots.*`, `version`, `keys.create`, `keys.get`, `keys.update`, `keys.delete`, `experimental.get`, `experimental.update`, `*`",
      "code": "invalid_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_actions"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_expires_at() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": {"name":"products"}
    });
    let (response, code) = server.add_api_key(content).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Invalid value type at `.expiresAt`: expected a string, but found an object: `{\"name\":\"products\"}`",
      "code": "invalid_api_key_expires_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_expires_at"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_expires_at_in_the_past() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2010-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;

    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Invalid value at `.expiresAt`: `2010-11-13T00:00:00Z` is not a valid date. It should follow the RFC 3339 format to represents a date or datetime in the future or specified as a null value. e.g. 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.\n",
      "code": "invalid_api_key_expires_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_expires_at"
    }
    "###);
    meili_snap::snapshot!(code, @"400 Bad Request");
}

#[actix_rt::test]
async fn error_add_api_key_invalid_parameters_uid() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "uid": "aaaaabbbbbccc",
        "indexes": ["products"],
        "actions": ["documents.add"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });
    let (response, code) = server.add_api_key(content).await;

    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Invalid value at `.uid`: invalid length: expected length 32 for simple format, found 13",
      "code": "invalid_api_key_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_uid"
    }
    "###);
    meili_snap::snapshot!(code, @"400 Bad Request");
}

#[actix_rt::test]
async fn error_add_api_key_parameters_uid_already_exist() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");
    let content: Value = json!({
        "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
        "indexes": ["products"],
        "actions": ["search"],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    // first creation is valid.
    let (response, code) = server.add_api_key(content.clone()).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": null,
      "key": "d9e776b8412f1db6974c9a5556b961c3559440b6588216f4ea5d9ed49f7c8f3c",
      "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
      "actions": [
        "search"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"201 Created");

    // uid already exist.
    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "`uid` field value `4bc0887a-0e41-4f3b-935d-0c451dcee9c8` is already an existing API key.",
      "code": "api_key_already_exists",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#api_key_already_exists"
    }
    "###);
    meili_snap::snapshot!(code, @"409 Conflict");
}

#[actix_rt::test]
async fn get_api_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let uid = "4bc0887a-0e41-4f3b-935d-0c451dcee9c8";
    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "uid": uid.to_string(),
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "Indexing API key",
      "key": "d9e776b8412f1db6974c9a5556b961c3559440b6588216f4ea5d9ed49f7c8f3c",
      "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "tasks.get",
        "settings.get",
        "settings.update",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"201 Created");

    let key = response["key"].as_str().unwrap();

    // get with uid
    let (response, code) = server.get_api_key(&uid).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "Indexing API key",
      "key": "d9e776b8412f1db6974c9a5556b961c3559440b6588216f4ea5d9ed49f7c8f3c",
      "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "tasks.get",
        "settings.get",
        "settings.update",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"200 OK");
    // get with key
    let (response, code) = server.get_api_key(&key).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "Indexing API key",
      "key": "d9e776b8412f1db6974c9a5556b961c3559440b6588216f4ea5d9ed49f7c8f3c",
      "uid": "4bc0887a-0e41-4f3b-935d-0c451dcee9c8",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "tasks.get",
        "settings.get",
        "settings.update",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"200 OK");
}

#[actix_rt::test]
async fn error_get_api_key_no_header() {
    let server = Server::new_auth().await;

    let (response, code) = server
        .get_api_key("d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "The Authorization header is missing. It must use the bearer authorization method.",
      "code": "missing_authorization_header",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");
}

#[actix_rt::test]
async fn error_get_api_key_bad_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("d4000bd7225f77d1eb22cc706ed36772bbc36767c016a27f76def7537b68600d");

    let (response, code) = server
        .get_api_key("d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);
    meili_snap::snapshot!(code, @"403 Forbidden");
}

#[actix_rt::test]
async fn error_get_api_key_not_found() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let (response, code) = server
        .get_api_key("d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "API key `d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4` not found.",
      "code": "api_key_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#api_key_not_found"
    }
    "###);
    meili_snap::snapshot!(code, @"404 Not Found");
}

#[actix_rt::test]
async fn list_api_keys() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "Indexing API key",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "tasks.get",
        "settings.get",
        "settings.update",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"201 Created");

    let (response, code) = server.list_api_keys("").await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".results[].createdAt" => "[ignored]", ".results[].updatedAt" => "[ignored]", ".results[].uid" => "[ignored]", ".results[].key" => "[ignored]" }), @r###"
    {
      "results": [
        {
          "name": null,
          "description": "Indexing API key",
          "key": "[ignored]",
          "uid": "[ignored]",
          "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create"
          ],
          "indexes": [
            "products"
          ],
          "expiresAt": "2050-11-13T00:00:00Z",
          "createdAt": "[ignored]",
          "updatedAt": "[ignored]"
        },
        {
          "name": "Default Search API Key",
          "description": "Use it to search from the frontend",
          "key": "[ignored]",
          "uid": "[ignored]",
          "actions": [
            "search"
          ],
          "indexes": [
            "*"
          ],
          "expiresAt": null,
          "createdAt": "[ignored]",
          "updatedAt": "[ignored]"
        },
        {
          "name": "Default Admin API Key",
          "description": "Use it for anything that is not a search operation. Caution! Do not expose it on a public frontend",
          "key": "[ignored]",
          "uid": "[ignored]",
          "actions": [
            "*"
          ],
          "indexes": [
            "*"
          ],
          "expiresAt": null,
          "createdAt": "[ignored]",
          "updatedAt": "[ignored]"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 3
    }
    "###);
    meili_snap::snapshot!(code, @"200 OK");
}

#[actix_rt::test]
async fn error_list_api_keys_no_header() {
    let server = Server::new_auth().await;

    let (response, code) = server.list_api_keys("").await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "The Authorization header is missing. It must use the bearer authorization method.",
      "code": "missing_authorization_header",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");
}

#[actix_rt::test]
async fn error_list_api_keys_bad_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("d4000bd7225f77d1eb22cc706ed36772bbc36767c016a27f76def7537b68600d");

    let (response, code) = server.list_api_keys("").await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);
    meili_snap::snapshot!(code, @"403 Forbidden");
}

#[actix_rt::test]
async fn delete_api_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "tasks.get",
            "settings.get",
            "settings.update",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "Indexing API key",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "tasks.get",
        "settings.get",
        "settings.update",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"201 Created");

    let uid = response["uid"].as_str().unwrap();

    let (response, code) = server.delete_api_key(&uid).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @"null");
    meili_snap::snapshot!(code, @"204 No Content");

    // check if API key no longer exist.
    let (response, code) = server.get_api_key(&uid).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".message" => "[ignored]" }), @r###"
    {
      "message": "[ignored]",
      "code": "api_key_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#api_key_not_found"
    }
    "###);
    meili_snap::snapshot!(code, @"404 Not Found");
}

#[actix_rt::test]
async fn error_delete_api_key_no_header() {
    let server = Server::new_auth().await;

    let (response, code) = server
        .delete_api_key("d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;

    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "The Authorization header is missing. It must use the bearer authorization method.",
      "code": "missing_authorization_header",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");
}

#[actix_rt::test]
async fn error_delete_api_key_bad_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("d4000bd7225f77d1eb22cc706ed36772bbc36767c016a27f76def7537b68600d");

    let (response, code) = server
        .delete_api_key("d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);
    meili_snap::snapshot!(code, @"403 Forbidden");
}

#[actix_rt::test]
async fn error_delete_api_key_not_found() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let (response, code) = server
        .delete_api_key("d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4")
        .await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "API key `d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4` not found.",
      "code": "api_key_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#api_key_not_found"
    }
    "###);
    meili_snap::snapshot!(code, @"404 Not Found");
}

#[actix_rt::test]
async fn patch_api_key_description() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]"  }), @r###"
    {
      "name": null,
      "description": null,
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"201 Created");

    let uid = response["uid"].as_str().unwrap();

    // Add a description and a name
    let content = json!({ "description": "Indexing API key", "name": "bob" });

    thread::sleep(time::Duration::new(1, 0));
    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": "bob",
      "description": "Indexing API key",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"200 OK");

    // Change the description
    let content = json!({ "description": "Product API key" });

    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": "bob",
      "description": "Product API key",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"200 OK");

    // Remove the description
    let content = json!({ "description": null });

    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": "bob",
      "description": null,
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"200 OK");
}

#[actix_rt::test]
async fn patch_api_key_name() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": null,
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"201 Created");

    let uid = response["uid"].as_str().unwrap();
    let created_at = response["createdAt"].as_str().unwrap();
    let updated_at = response["updatedAt"].as_str().unwrap();

    // Add a name and description
    let content = json!({ "name": "Indexing API key", "description": "The doggoscription" });

    thread::sleep(time::Duration::new(1, 0));
    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": "Indexing API key",
      "description": "The doggoscription",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"200 OK");

    assert_ne!(response["updatedAt"].as_str().unwrap(), updated_at);
    assert_eq!(response["createdAt"].as_str().unwrap(), created_at);

    // Change the name
    let content = json!({ "name": "Product API key" });

    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": "Product API key",
      "description": "The doggoscription",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"200 OK");

    // Remove the name
    let content = json!({ "name": null });

    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "The doggoscription",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"200 OK");
}

#[actix_rt::test]
async fn error_patch_api_key_indexes() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "Indexing API key",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"201 Created");

    let uid = response["uid"].as_str().unwrap();

    let content = json!({ "indexes": ["products", "prices"] });

    thread::sleep(time::Duration::new(1, 0));
    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Immutable field `indexes`: expected one of `description`, `name`",
      "code": "immutable_api_key_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_indexes"
    }
    "###);
    meili_snap::snapshot!(code, @"400 Bad Request");
}

#[actix_rt::test]
async fn error_patch_api_key_actions() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "Indexing API key",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"201 Created");

    let uid = response["uid"].as_str().unwrap();

    let content = json!({
        "actions": [
            "search",
            "documents.get",
            "indexes.get",
            "tasks.get",
            "settings.get",
        ],
    });

    thread::sleep(time::Duration::new(1, 0));
    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Immutable field `actions`: expected one of `description`, `name`",
      "code": "immutable_api_key_actions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_actions"
    }
    "###);
    meili_snap::snapshot!(code, @"400 Bad Request");
}

#[actix_rt::test]
async fn error_patch_api_key_expiration_date() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
            "documents.add",
            "documents.get",
            "documents.delete",
            "indexes.create",
            "indexes.get",
            "indexes.update",
            "indexes.delete",
            "stats.get",
            "dumps.create",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "Indexing API key",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search",
        "documents.add",
        "documents.get",
        "documents.delete",
        "indexes.create",
        "indexes.get",
        "indexes.update",
        "indexes.delete",
        "stats.get",
        "dumps.create"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"201 Created");

    let uid = response["uid"].as_str().unwrap();

    let content = json!({ "expiresAt": "2055-11-13T00:00:00Z" });

    thread::sleep(time::Duration::new(1, 0));
    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Immutable field `expiresAt`: expected one of `description`, `name`",
      "code": "immutable_api_key_expires_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#immutable_api_key_expires_at"
    }
    "###);
    meili_snap::snapshot!(code, @"400 Bad Request");
}

#[actix_rt::test]
async fn error_patch_api_key_no_header() {
    let server = Server::new_auth().await;

    let (response, code) = server
        .patch_api_key(
            "d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4",
            json!({}),
        )
        .await;

    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "The Authorization header is missing. It must use the bearer authorization method.",
      "code": "missing_authorization_header",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");
}

#[actix_rt::test]
async fn error_patch_api_key_bad_key() {
    let mut server = Server::new_auth().await;
    server.use_api_key("d4000bd7225f77d1eb22cc706ed36772bbc36767c016a27f76def7537b68600d");

    let (response, code) = server
        .patch_api_key(
            "d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4",
            json!({}),
        )
        .await;

    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "The provided API key is invalid.",
      "code": "invalid_api_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key"
    }
    "###);
    meili_snap::snapshot!(code, @"403 Forbidden");
}

#[actix_rt::test]
async fn error_patch_api_key_not_found() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let (response, code) = server
        .patch_api_key(
            "d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4",
            json!({}),
        )
        .await;

    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "API key `d0552b41d0552b41536279a0ad88bd595327b96f01176a60c2243e906c52ac02375f9bc4` not found.",
      "code": "api_key_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#api_key_not_found"
    }
    "###);
    meili_snap::snapshot!(code, @"404 Not Found");
}

#[actix_rt::test]
async fn error_patch_api_key_indexes_invalid_parameters() {
    let mut server = Server::new_auth().await;
    server.use_api_key("MASTER_KEY");

    let content = json!({
        "description": "Indexing API key",
        "indexes": ["products"],
        "actions": [
            "search",
        ],
        "expiresAt": "2050-11-13T00:00:00Z"
    });

    let (response, code) = server.add_api_key(content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]", ".uid" => "[ignored]", ".key" => "[ignored]" }), @r###"
    {
      "name": null,
      "description": "Indexing API key",
      "key": "[ignored]",
      "uid": "[ignored]",
      "actions": [
        "search"
      ],
      "indexes": [
        "products"
      ],
      "expiresAt": "2050-11-13T00:00:00Z",
      "createdAt": "[ignored]",
      "updatedAt": "[ignored]"
    }
    "###);
    meili_snap::snapshot!(code, @"201 Created");

    let uid = response["uid"].as_str().unwrap();

    // invalid description
    let content = json!({
        "description": 13
    });

    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Invalid value type at `.description`: expected a string, but found a positive integer: `13`",
      "code": "invalid_api_key_description",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_description"
    }
    "###);
    meili_snap::snapshot!(code, @"400 Bad Request");

    // invalid name
    let content = json!({
        "name": 13
    });

    let (response, code) = server.patch_api_key(&uid, content).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Invalid value type at `.name`: expected a string, but found a positive integer: `13`",
      "code": "invalid_api_key_name",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_api_key_name"
    }
    "###);
    meili_snap::snapshot!(code, @"400 Bad Request");
}

#[actix_rt::test]
async fn error_access_api_key_routes_no_master_key_set() {
    let mut server = Server::new().await;

    let (response, code) = server.add_api_key(json!({})).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
      "code": "missing_master_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_master_key"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");

    let (response, code) = server.patch_api_key("content", json!({})).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
      "code": "missing_master_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_master_key"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");

    let (response, code) = server.get_api_key("content").await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
      "code": "missing_master_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_master_key"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");

    let (response, code) = server.list_api_keys("").await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
      "code": "missing_master_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_master_key"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");

    server.use_api_key("MASTER_KEY");

    let (response, code) = server.add_api_key(json!({})).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
      "code": "missing_master_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_master_key"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");

    let (response, code) = server.patch_api_key("content", json!({})).await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
      "code": "missing_master_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_master_key"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");

    let (response, code) = server.get_api_key("content").await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
      "code": "missing_master_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_master_key"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");

    let (response, code) = server.list_api_keys("").await;
    meili_snap::snapshot!(meili_snap::json_string!(response, { ".createdAt" => "[ignored]", ".updatedAt" => "[ignored]" }), @r###"
    {
      "message": "Meilisearch is running without a master key. To access this API endpoint, you must have set a master key at launch.",
      "code": "missing_master_key",
      "type": "auth",
      "link": "https://docs.meilisearch.com/errors#missing_master_key"
    }
    "###);
    meili_snap::snapshot!(code, @"401 Unauthorized");
}
