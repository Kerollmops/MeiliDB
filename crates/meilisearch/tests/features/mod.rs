use meilisearch::Opt;
use tempfile::TempDir;

use crate::common::{default_settings, Server};
use crate::json;

/// Feature name to test against.
/// This will have to be changed by a different one when that feature is stabilized.
/// All tests that need to set a feature can make use of this constant.
const FEATURE_NAME: &str = "metrics";

#[actix_rt::test]
async fn experimental_features() {
    let server = Server::new().await;

    let (response, code) = server.get_features().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": false,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false
    }
    "###);

    let (response, code) = server.set_features(json!({FEATURE_NAME: true})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": true,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false
    }
    "###);

    let (response, code) = server.get_features().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": true,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false
    }
    "###);

    // sending null does not change the value
    let (response, code) = server.set_features(json!({FEATURE_NAME: null})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": true,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false
    }
    "###);

    // not sending the field does not change the value
    let (response, code) = server.set_features(json!({})).await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": true,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false
    }
    "###);
}

#[actix_rt::test]
async fn experimental_feature_metrics() {
    // instance flag for metrics enables metrics at startup
    let dir = TempDir::new().unwrap();
    let enable_metrics = Opt { experimental_enable_metrics: true, ..default_settings(dir.path()) };
    let server = Server::new_with_options(enable_metrics).await.unwrap();

    let (response, code) = server.get_features().await;

    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "metrics": true,
      "logsRoute": false,
      "editDocumentsByFunction": false,
      "containsFilter": false
    }
    "###);

    let (response, code) = server.get_metrics().await;
    meili_snap::snapshot!(code, @"200 OK");

    // metrics are not returned in json format
    // so the test server will return null
    meili_snap::snapshot!(response, @"null");

    // disabling metrics results in invalid request
    let (response, code) = server.set_features(json!({"metrics": false})).await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(response["metrics"], @"false");

    let (response, code) = server.get_metrics().await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Getting metrics requires enabling the `metrics` experimental feature. See https://github.com/meilisearch/product/discussions/625",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    // enabling metrics via HTTP results in valid request
    let (response, code) = server.set_features(json!({"metrics": true})).await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(response["metrics"], @"true");

    let (response, code) = server.get_metrics().await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(response, @"null");

    // startup without flag respects persisted metrics value
    let disable_metrics =
        Opt { experimental_enable_metrics: false, ..default_settings(dir.path()) };
    let server_no_flag = Server::new_with_options(disable_metrics).await.unwrap();
    let (response, code) = server_no_flag.get_metrics().await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(response, @"null");
}

#[actix_rt::test]
async fn errors() {
    let server = Server::new().await;

    // Sending a feature not in the list is an error
    let (response, code) = server.set_features(json!({"NotAFeature": true})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Unknown field `NotAFeature`: expected one of `metrics`, `logsRoute`, `editDocumentsByFunction`, `containsFilter`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // The type must be a bool, not a number
    let (response, code) = server.set_features(json!({FEATURE_NAME: 42})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Invalid value type at `.metrics`: expected a boolean, but found a positive integer: `42`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // The type must be a bool, not a string
    let (response, code) = server.set_features(json!({FEATURE_NAME: "true"})).await;

    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Invalid value type at `.metrics`: expected a boolean, but found a string: `\"true\"`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}
