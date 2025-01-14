use meili_snap::{json_string, snapshot};

use crate::common::{GetAllDocumentsOptions, Server};
use crate::json;
use crate::vector::generate_default_user_provided_documents;

#[actix_rt::test]
async fn field_unavailable_for_source() {
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": { "manual": {"source": "userProvided", "documentTemplate": "{{doc.documentTemplate}}"}},
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.manual`: Field `documentTemplate` unavailable for source `userProvided` (only available for sources: `huggingFace`, `openAi`, `ollama`, `rest`). Available fields: `source`, `dimensions`, `distribution`, `binaryQuantized`",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);

    let (response, code) = index
        .update_settings(json!({
          "embedders": { "default": {"source": "openAi", "revision": "42"}},
        }))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "`.embedders.default`: Field `revision` unavailable for source `openAi` (only available for sources: `huggingFace`). Available fields: `source`, `model`, `apiKey`, `documentTemplate`, `dimensions`, `distribution`, `url`, `binaryQuantized`",
      "code": "invalid_settings_embedders",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_embedders"
    }
    "###);
}

#[actix_rt::test]
async fn update_embedder() {
    let server = Server::new().await;
    let index = server.index("doggo");

    let (response, code) = index
        .update_settings(json!({
          "embedders": { "manual": {}},
        }))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index
        .update_settings(json!({
          "embedders": {
              "manual": {
                  "source": "userProvided",
                  "dimensions": 2,
              }
          },
        }))
        .await;
    snapshot!(code, @"202 Accepted");

    let ret = server.wait_task(response.uid()).await;
    snapshot!(ret, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "embedders": {
          "manual": {
            "source": "userProvided",
            "dimensions": 2
          }
        }
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn reset_embedder_documents() {
    let server = Server::new().await;
    let index = generate_default_user_provided_documents(&server).await;

    let (response, code) = index.delete_settings().await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    // Make sure the documents are still present
    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions {
            limit: None,
            offset: None,
            retrieve_vectors: false,
            fields: None,
        })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 0,
          "name": "kefir"
        },
        {
          "id": 1,
          "name": "echo"
        },
        {
          "id": 2,
          "name": "billou"
        },
        {
          "id": 3,
          "name": "intel"
        },
        {
          "id": 4,
          "name": "max"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 5
    }
    "###);

    // Make sure we are still able to retrieve their vectors
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
                  0.0,
                  0.0,
                  0.0
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
                  1.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 2,
          "name": "billou",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  2.0,
                  2.0,
                  2.0
                ],
                [
                  2.0,
                  2.0,
                  3.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 3,
          "name": "intel",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  3.0,
                  3.0,
                  3.0
                ]
              ],
              "regenerate": false
            }
          }
        },
        {
          "id": 4,
          "name": "max",
          "_vectors": {
            "manual": {
              "embeddings": [
                [
                  4.0,
                  4.0,
                  4.0
                ],
                [
                  4.0,
                  4.0,
                  5.0
                ]
              ],
              "regenerate": false
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 5
    }
    "###);

    // Make sure the arroy DB has been cleared
    let (documents, _code) =
        index.search_post(json!({ "vector": [1, 1, 1], "hybrid": {"embedder": "default"} })).await;
    snapshot!(json_string!(documents), @r###"
    {
      "message": "Cannot find embedder with name `default`.",
      "code": "invalid_search_embedder",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_embedder"
    }
    "###);
}
