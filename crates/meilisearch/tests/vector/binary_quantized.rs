use meili_snap::{json_string, snapshot};

use crate::common::{GetAllDocumentsOptions, Server};
use crate::json;
use crate::vector::generate_default_user_provided_documents;

#[actix_rt::test]
async fn retrieve_binary_quantize_status_in_the_settings() {
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 3,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(settings["embedders"]["manual"], @r#"{"source":"userProvided","dimensions":3}"#);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 3,
                  "binaryQuantized": false,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(settings["embedders"]["manual"], @r#"{"source":"userProvided","dimensions":3,"binaryQuantized":false}"#);

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 3,
                  "binaryQuantized": true,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(settings["embedders"]["manual"], @r#"{"source":"userProvided","dimensions":3,"binaryQuantized":true}"#);
}

#[actix_rt::test]
async fn binary_quantize_before_sending_documents() {
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 3,
                  "binaryQuantized": true,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [-1.2, -2.3, 3.2] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": [2.5, 1.5, -130] }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    // Make sure the documents are binary quantized
    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  -1.0,
                  -1.0,
                  1.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 1,
          "name": "echo",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  -1.0
                ]
              ],
              "regenerate": false
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
}

#[actix_rt::test]
async fn binary_quantize_after_sending_documents() {
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 3,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [-1.2, -2.3, 3.2] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": [2.5, 1.5, -130] }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 3,
                  "binaryQuantized": true,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    // Make sure the documents are binary quantized
    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  -1.0,
                  -1.0,
                  1.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 1,
          "name": "echo",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  1.0,
                  1.0,
                  -1.0
                ]
              ],
              "regenerate": false
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
}

#[actix_rt::test]
async fn try_to_disable_binary_quantization() {
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 3,
                  "binaryQuantized": true,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 3,
                  "binaryQuantized": false,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    let ret = server.wait_task(response.uid()).await;
    snapshot!(ret, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "manual": {
            "source": "userProvided",
            "dimensions": 3,
            "binaryQuantized": false
          }
        }
      },
      "error": {
        "message": "Index `doggo`: `.embedders.manual.binaryQuantized`: Cannot disable the binary quantization.\n - Note: Binary quantization is a lossy operation that cannot be reverted.\n - Hint: Add a new embedder that is non-quantized and regenerate the vectors.",
        "code": "invalid_settings_embedders",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);
}

#[actix_rt::test]
async fn binary_quantize_clear_documents() {
    let server = Server::new().await;
    let index = generate_default_user_provided_documents(&server).await;

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "binaryQuantized": true,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let (value, _code) = index.clear_all_documents().await;
    index.wait_task(value.uid()).await.succeeded();

    // Make sure the documents DB has been cleared
    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [],
      "offset": 0,
      "limit": 20,
      "total": 0
    }
    "###);

    // Make sure the arroy DB has been cleared
    let (documents, _code) =
        index.search_post(json!({ "hybrid": { "embedder": "manual" }, "vector": [1, 1, 1] })).await;
    snapshot!(documents, @r###"
    {
      "hits": [],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0,
      "semanticHitCount": 0
    }
    "###);
}
