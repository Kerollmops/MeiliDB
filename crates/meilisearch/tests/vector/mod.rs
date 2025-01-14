mod binary_quantized;
mod openai;
mod rest;
mod settings;

use std::str::FromStr;

use meili_snap::{json_string, snapshot};
use meilisearch::option::MaxThreads;

use crate::common::index::Index;
use crate::common::{default_settings, GetAllDocumentsOptions, Server};
use crate::json;

async fn get_server_vector() -> Server {
    Server::new().await
}

#[actix_rt::test]
async fn add_remove_user_provided() {
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
      {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": [1, 1, 1] }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

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
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [10, 10, 10] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": null }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

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
                  10.0,
                  10.0,
                  10.0
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
              "embeddings": [],
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

    let (value, code) = index.delete_document(0).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let (documents, _code) = index
        .get_all_documents(GetAllDocumentsOptions { retrieve_vectors: true, ..Default::default() })
        .await;
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "id": 1,
          "name": "echo",
          "_vectors": {
            "manual": {
              "embeddings": [],
              "regenerate": false
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);
}

async fn generate_default_user_provided_documents(server: &Server) -> Index {
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
    server.wait_task(response.uid()).await;

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0] }},
      {"id": 1, "name": "echo", "_vectors": { "manual": [1, 1, 1] }},
      {"id": 2, "name": "billou", "_vectors": { "manual": [[2, 2, 2], [2, 2, 3]] }},
      {"id": 3, "name": "intel", "_vectors": { "manual": { "regenerate": false, "embeddings": [3, 3, 3] }}},
      {"id": 4, "name": "max", "_vectors": { "manual": { "regenerate": false, "embeddings": [[4, 4, 4], [4, 4, 5]] }}},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    index
}

#[actix_rt::test]
async fn user_provided_embeddings_error() {
    let server = Server::new().await;
    let index = generate_default_user_provided_documents(&server).await;

    // First case, we forget to specify the `regenerate`
    let documents =
        json!({"id": 0, "name": "kefir", "_vectors": { "manual": { "embeddings": [0, 0, 0] }}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: Bad embedder configuration in the document with id: `0`. Missing field `._vectors.manual.regenerate`\n  - note: `._vectors.manual` must be an array of floats, an array of arrays of floats, or an object with field `regenerate`",
        "code": "invalid_vectors_type",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_vectors_type"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // Second case, we don't specify anything
    let documents = json!({"id": 0, "name": "kefir", "_vectors": { "manual": {}}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: Bad embedder configuration in the document with id: `0`. Missing field `._vectors.manual.regenerate`\n  - note: `._vectors.manual` must be an array of floats, an array of arrays of floats, or an object with field `regenerate`",
        "code": "invalid_vectors_type",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_vectors_type"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // Third case, we specify something wrong in place of regenerate
    let documents =
        json!({"id": 0, "name": "kefir", "_vectors": { "manual": { "regenerate": "yes please" }}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: Bad embedder configuration in the document with id: `0`. Could not parse `._vectors.manual.regenerate`: invalid type: string \"yes please\", expected a boolean at line 1 column 26",
        "code": "invalid_vectors_type",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_vectors_type"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let documents = json!({"id": 0, "name": "kefir", "_vectors": { "manual": { "embeddings": true, "regenerate": true }}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: Bad embedder configuration in the document with id: `0`. Invalid value type at `._vectors.manual.embeddings`: expected null or an array, but found a boolean: `true`",
        "code": "invalid_vectors_type",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_vectors_type"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let documents = json!({"id": 0, "name": "kefir", "_vectors": { "manual": { "embeddings": [true], "regenerate": true }}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: Bad embedder configuration in the document with id: `0`. Invalid value type at `._vectors.manual.embeddings[0]`: expected a number or an array, but found a boolean: `true`",
        "code": "invalid_vectors_type",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_vectors_type"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let documents = json!({"id": 0, "name": "kefir", "_vectors": { "manual": { "embeddings": [[true]], "regenerate": false }}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: Bad embedder configuration in the document with id: `0`. Invalid value type at `._vectors.manual.embeddings[0][0]`: expected a number, but found a boolean: `true`",
        "code": "invalid_vectors_type",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_vectors_type"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let documents = json!({"id": 0, "name": "kefir", "_vectors": { "manual": { "embeddings": [23, 0.1, -12], "regenerate": true }}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);

    let documents =
        json!({"id": 0, "name": "kefir", "_vectors": { "manual": { "regenerate": false }}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task["status"], @r###""succeeded""###);

    let documents = json!({"id": 0, "name": "kefir", "_vectors": { "manual": { "regenerate": false, "embeddings": [0.1, [0.2, 0.3]] }}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: Bad embedder configuration in the document with id: `0`. Invalid value type at `._vectors.manual.embeddings[1]`: expected a number, but found an array: `[0.2,0.3]`",
        "code": "invalid_vectors_type",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_vectors_type"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let documents = json!({"id": 0, "name": "kefir", "_vectors": { "manual": { "regenerate": false, "embeddings": [[0.1, 0.2], 0.3] }}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: Bad embedder configuration in the document with id: `0`. Invalid value type at `._vectors.manual.embeddings[1]`: expected an array, but found a number: `0.3`",
        "code": "invalid_vectors_type",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_vectors_type"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let documents = json!({"id": 0, "name": "kefir", "_vectors": { "manual": { "regenerate": false, "embeddings": [[0.1, true], 0.3] }}});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: Bad embedder configuration in the document with id: `0`. Invalid value type at `._vectors.manual.embeddings[0][1]`: expected a number, but found a boolean: `true`",
        "code": "invalid_vectors_type",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_vectors_type"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn user_provided_vectors_error() {
    let temp = tempfile::tempdir().unwrap();
    let mut options = default_settings(temp.path());
    // If we have more than one indexing thread the error messages below may become inconsistent
    options.indexer_options.max_indexing_threads = MaxThreads::from_str("1").unwrap();
    let server = Server::new_with_options(options).await.unwrap();

    let index = generate_default_user_provided_documents(&server).await;

    // First case, we forget to specify `_vectors`
    let documents = json!([{"id": 40, "name": "kefir"}, {"id": 41, "name": "intel"}, {"id": 42, "name": "max"}, {"id": 43, "name": "venus"}, {"id": 44, "name": "eva"}]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 5,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: While embedding documents for embedder `manual`: no vectors provided for document `40` and at least 4 other document(s)\n- Note: `manual` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.manual`.\n- Hint: opt-out for a document with `_vectors.manual: null`",
        "code": "vector_embedding_error",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // Second case, we provide `_vectors` with a typo
    let documents = json!({"id": 42, "name": "kefir", "_vector": { "manaul": [0, 0, 0] }});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: While embedding documents for embedder `manual`: no vectors provided for document `42`\n- Note: `manual` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.manual`.\n- Hint: try replacing `_vector` by `_vectors` in 1 document(s).",
        "code": "vector_embedding_error",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // Third case, we specify the embedder with a typo
    let documents = json!({"id": 42, "name": "kefir", "_vectors": { "manaul": [0, 0, 0] }});
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "doggo",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `doggo`: While embedding documents for embedder `manual`: no vectors provided for document `42`\n- Note: `manual` has `source: userProvided`, so documents must provide embeddings as an array in `_vectors.manual`.\n- Hint: try replacing `_vectors.manaul` by `_vectors.manual` in 1 document(s).",
        "code": "vector_embedding_error",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn clear_documents() {
    let server = Server::new().await;
    let index = generate_default_user_provided_documents(&server).await;

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
        index.search_post(json!({ "vector": [1, 1, 1], "hybrid": {"embedder": "manual"} })).await;
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

#[actix_rt::test]
async fn add_remove_one_vector_4588() {
    // https://github.com/meilisearch/meilisearch/issues/4588
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
    let task = server.wait_task(response.uid()).await;
    snapshot!(task, name: "settings-processed");

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": [0, 0, 0] }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, name: "document-added");

    let documents = json!([
      {"id": 0, "name": "kefir", "_vectors": { "manual": null }},
    ]);
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let task = index.wait_task(value.uid()).await;
    snapshot!(task, name: "document-deleted");

    let (documents, _code) = index
        .search_post(
            json!({"vector": [1, 1, 1], "hybrid": {"semanticRatio": 1.0, "embedder": "manual"} }),
        )
        .await;
    snapshot!(documents, @r###"
    {
      "hits": [
        {
          "id": 0,
          "name": "kefir"
        }
      ],
      "query": "",
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 1,
      "semanticHitCount": 1
    }
    "###);

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
              "embeddings": [],
              "regenerate": false
            }
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "###);
}
