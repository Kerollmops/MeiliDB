use meili_snap::{json_string, snapshot};

use super::{DOCUMENTS, FRUITS_DOCUMENTS, NESTED_DOCUMENTS};
use crate::common::Server;
use crate::json;
use crate::search::{SCORE_DOCUMENTS, VECTOR_DOCUMENTS};

#[actix_rt::test]
async fn search_empty_list() {
    let server = Server::new().await;

    let (response, code) = server.multi_search(json!({"queries": []})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "results": []
    }
    "###);
}

#[actix_rt::test]
async fn federation_empty_list() {
    let server = Server::new().await;

    let (response, code) = server.multi_search(json!({"federation": {}, "queries": []})).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, {".processingTimeMs" => "[time]"}), @r###"
    {
      "hits": [],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 0
    }
    "###);
}

#[actix_rt::test]
async fn search_json_object() {
    let server = Server::new().await;

    let (response, code) = server.multi_search(json!({})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `queries`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn federation_no_queries() {
    let server = Server::new().await;

    let (response, code) = server.multi_search(json!({"federation": {}})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Missing field `queries`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn search_json_array() {
    let server = Server::new().await;

    let (response, code) = server.multi_search(json!([])).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an object, but found an array: `[]`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn simple_search_single_index() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid": "test", "q": "glass"},
        {"indexUid": "test", "q": "captain"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response["results"], { "[].processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    [
      {
        "indexUid": "test",
        "hits": [
          {
            "title": "Gläss",
            "id": "450465",
            "color": [
              "blue",
              "red"
            ]
          }
        ],
        "query": "glass",
        "processingTimeMs": "[time]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 1
      },
      {
        "indexUid": "test",
        "hits": [
          {
            "title": "Captain Marvel",
            "id": "299537",
            "color": [
              "yellow",
              "blue"
            ]
          }
        ],
        "query": "captain",
        "processingTimeMs": "[time]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 1
      }
    ]
    "###);
}

#[actix_rt::test]
async fn federation_single_search_single_index() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "test", "q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    {
      "hits": [
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 1
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_search_single_index() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = SCORE_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid": "test", "q": "the bat"},
        {"indexUid": "test", "q": "badman returns"},
        {"indexUid" : "test", "q": "batman"},
        {"indexUid": "test", "q": "batman returns"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    {
      "hits": [
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 3,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.5
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 5
    }
    "###);
}

#[actix_rt::test]
async fn federation_two_search_single_index() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "test", "q": "captain"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    {
      "hits": [
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 2
    }
    "###);
}

#[actix_rt::test]
async fn simple_search_missing_index_uid() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, @r###"
    {
      "message": "Missing field `indexUid` inside `.queries[0]`",
      "code": "missing_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn federation_simple_search_missing_index_uid() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, @r###"
    {
      "message": "Missing field `indexUid` inside `.queries[0]`",
      "code": "missing_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn simple_search_illegal_index_uid() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid": "hé", "q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, @r###"
    {
      "message": "Invalid value at `.queries[0].indexUid`: `hé` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn federation_search_illegal_index_uid() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid": "hé", "q": "glass"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, @r###"
    {
      "message": "Invalid value at `.queries[0].indexUid`: `hé` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn simple_search_two_indexes() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (add_task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(add_task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response["results"], { "[].processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    [
      {
        "indexUid": "test",
        "hits": [
          {
            "title": "Gläss",
            "id": "450465",
            "color": [
              "blue",
              "red"
            ]
          }
        ],
        "query": "glass",
        "processingTimeMs": "[time]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 1
      },
      {
        "indexUid": "nested",
        "hits": [
          {
            "id": 852,
            "father": "jean",
            "mother": "michelle",
            "doggos": [
              {
                "name": "bobby",
                "age": 2
              },
              {
                "name": "buddy",
                "age": 4
              }
            ],
            "cattos": "pésti"
          },
          {
            "id": 654,
            "father": "pierre",
            "mother": "sabine",
            "doggos": [
              {
                "name": "gros bill",
                "age": 8
              }
            ],
            "cattos": [
              "simba",
              "pestiféré"
            ]
          }
        ],
        "query": "pésti",
        "processingTimeMs": "[time]",
        "limit": 20,
        "offset": 0,
        "estimatedTotalHits": 2
      }
    ]
    "###);
}

#[actix_rt::test]
async fn federation_two_search_two_indexes() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    {
      "hits": [
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7803030303030303
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_search_multiple_indexes() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("score");
    let documents = SCORE_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid" : "test", "q": "captain"},
        {"indexUid": "nested", "q": "pésti"},
        {"indexUid" : "test", "q": "Escape"},
        {"indexUid": "nested", "q": "jean"},
        {"indexUid": "score", "q": "jean"},
        {"indexUid": "test", "q": "the bat"},
        {"indexUid": "score", "q": "the bat"},
        {"indexUid": "score", "q": "badman returns"},
        {"indexUid" : "score", "q": "batman"},
        {"indexUid": "score", "q": "batman returns"},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    {
      "hits": [
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "score",
            "queriesPosition": 9,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "score",
            "queriesPosition": 10,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Escape Room",
          "id": "522681",
          "color": [
            "yellow",
            "red"
          ],
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 3,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "id": 951,
          "father": "jean-baptiste",
          "mother": "sophie",
          "doggos": [
            {
              "name": "turbo",
              "age": 5
            },
            {
              "name": "fast",
              "age": 6
            }
          ],
          "cattos": [
            "moumoute",
            "gomez"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 4,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "score",
            "queriesPosition": 9,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "score",
            "queriesPosition": 9,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 2,
            "weightedRankingScore": 0.7803030303030303
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "score",
            "queriesPosition": 8,
            "weightedRankingScore": 0.5
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "id": "166428",
          "color": [
            "green",
            "red"
          ],
          "_federation": {
            "indexUid": "test",
            "queriesPosition": 6,
            "weightedRankingScore": 0.4166666666666667
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 12
    }
    "###);
}

#[actix_rt::test]
async fn search_one_index_doesnt_exist() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `nested` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);
}

#[actix_rt::test]
async fn federation_one_index_doesnt_exist() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `nested` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);
}

#[actix_rt::test]
async fn search_multiple_indexes_dont_exist() {
    let server = Server::new().await;

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Index `test` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_indexes_dont_exist() {
    let server = Server::new().await;

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    // order of indexes that are not found depends on the alphabetical order of index names
    // the query index is the lowest index with that index
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `nested` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);
}

#[actix_rt::test]
async fn search_one_query_error() {
    let server = Server::new().await;

    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass", "facets": ["title"]},
        {"indexUid": "nested", "q": "pésti"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Invalid facet distribution, this index does not have configured filterable attributes.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
}

#[actix_rt::test]
async fn federation_one_query_error() {
    let server = Server::new().await;

    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti", "filter": ["title = toto"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `nested`: Attribute `title` is not filterable. This index does not have configured filterable attributes.\n1:6 title = toto",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "###);
}

#[actix_rt::test]
async fn federation_one_query_sort_error() {
    let server = Server::new().await;

    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti", "sort": ["doggos:desc"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `nested`: Attribute `doggos` is not sortable. This index does not have configured sortable attributes.",
      "code": "invalid_search_sort",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    }
    "###);
}

#[actix_rt::test]
async fn search_multiple_query_errors() {
    let server = Server::new().await;

    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass", "facets": ["title"]},
        {"indexUid": "nested", "q": "pésti", "facets": ["doggos"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Invalid facet distribution, this index does not have configured filterable attributes.",
      "code": "invalid_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_facets"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_query_errors() {
    let server = Server::new().await;

    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass", "filter": ["title = toto"]},
        {"indexUid": "nested", "q": "pésti", "filter": ["doggos IN [intel, kefir]"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Index `test`: Attribute `title` is not filterable. This index does not have configured filterable attributes.\n1:6 title = toto",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_query_sort_errors() {
    let server = Server::new().await;

    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass", "sort": ["title:desc"]},
        {"indexUid": "nested", "q": "pésti", "sort": ["doggos:desc"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[0]`: Index `test`: Attribute `title` is not sortable. This index does not have configured sortable attributes.",
      "code": "invalid_search_sort",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_query_errors_interleaved() {
    let server = Server::new().await;

    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti", "filter": ["doggos IN [intel, kefir]"]},
        {"indexUid" : "test", "q": "glass", "filter": ["title = toto"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `nested`: Attribute `doggos` is not filterable. This index does not have configured filterable attributes.\n1:7 doggos IN [intel, kefir]",
      "code": "invalid_search_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
    }
    "###);
}

#[actix_rt::test]
async fn federation_multiple_query_sort_errors_interleaved() {
    let server = Server::new().await;

    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "test", "q": "glass"},
        {"indexUid": "nested", "q": "pésti", "sort": ["doggos:desc"]},
        {"indexUid" : "test", "q": "glass", "sort": ["title:desc"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Inside `.queries[1]`: Index `nested`: Attribute `doggos` is not sortable. This index does not have configured sortable attributes.",
      "code": "invalid_search_sort",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_sort"
    }
    "###);
}

#[actix_rt::test]
async fn federation_filter() {
    let server = Server::new().await;

    let index = server.index("fruits");

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST"]}),
        )
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "fruits", "q": "apple red", "filter": "BOOST = true", "showRankingScore": true, "federationOptions": {"weight": 3.0}},
        {"indexUid": "fruits", "q": "apple red", "showRankingScore": true},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "name": "Exclusive sale: Red delicious apple",
          "id": "red-delicious-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "fruits",
            "queriesPosition": 0,
            "weightedRankingScore": 2.7281746031746033
          },
          "_rankingScore": 0.9093915343915344
        },
        {
          "name": "Exclusive sale: green apple",
          "id": "green-apple-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "fruits",
            "queriesPosition": 0,
            "weightedRankingScore": 1.318181818181818
          },
          "_rankingScore": 0.4393939393939394
        },
        {
          "name": "Red apple gala",
          "id": "red-apple-gala",
          "_federation": {
            "indexUid": "fruits",
            "queriesPosition": 1,
            "weightedRankingScore": 0.953042328042328
          },
          "_rankingScore": 0.953042328042328
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_same_indexes_same_criterion_same_direction() {
    let server = Server::new().await;

    let index = server.index("nested");

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["mother"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // two identical placeholder search should have all results from first query
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "nested", "q": "", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : "nested", "q": "", "sort": ["mother:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 750,
          "father": "romain",
          "mother": "michelle",
          "cattos": [
            "enigma"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 951,
          "father": "jean-baptiste",
          "mother": "sophie",
          "doggos": [
            {
              "name": "turbo",
              "age": 5
            },
            {
              "name": "fast",
              "age": 6
            }
          ],
          "cattos": [
            "moumoute",
            "gomez"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4
    }
    "###);

    // mix and match query
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "nested", "q": "pésti", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : "nested", "q": "jean", "sort": ["mother:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7803030303030303
          },
          "_rankingScore": 0.7803030303030303
        },
        {
          "id": 951,
          "father": "jean-baptiste",
          "mother": "sophie",
          "doggos": [
            {
              "name": "turbo",
              "age": 5
            },
            {
              "name": "fast",
              "age": 6
            }
          ],
          "cattos": [
            "moumoute",
            "gomez"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          },
          "_rankingScore": 0.9848484848484848
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_same_indexes_same_criterion_opposite_direction() {
    let server = Server::new().await;

    let index = server.index("nested");

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["mother"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // two identical placeholder search should have all results from first query
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "nested", "q": "", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : "nested", "q": "", "sort": ["mother:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #0 and #1 are incompatible: \n  1. `queries[0].sort[0]`, `nested.rankingRules[0]`: ascending sort rule(s) on field `mother`\n  2. `queries[1].sort[0]`, `nested.rankingRules[0]`: descending sort rule(s) on field `mother`\n  - cannot compare two sort rules in opposite directions\n  - note: The ranking rules of query #0 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n  - note: The ranking rules of query #1 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);

    // mix and match query: should be ranked by ranking score
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "nested", "q": "pésti", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : "nested", "q": "jean", "sort": ["mother:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #0 and #1 are incompatible: \n  1. `queries[0].sort[0]`, `nested.rankingRules[0]`: ascending sort rule(s) on field `mother`\n  2. `queries[1].sort[0]`, `nested.rankingRules[0]`: descending sort rule(s) on field `mother`\n  - cannot compare two sort rules in opposite directions\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_same_indexes_different_criterion_same_direction() {
    let server = Server::new().await;

    let index = server.index("nested");

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["mother", "father"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // return mothers and fathers ordered accross fields.
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "nested", "q": "", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : "nested", "q": "", "sort": ["father:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 951,
          "father": "jean-baptiste",
          "mother": "sophie",
          "doggos": [
            {
              "name": "turbo",
              "age": 5
            },
            {
              "name": "fast",
              "age": 6
            }
          ],
          "cattos": [
            "moumoute",
            "gomez"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 750,
          "father": "romain",
          "mother": "michelle",
          "cattos": [
            "enigma"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4
    }
    "###);

    // mix and match query: will be sorted across mother and father names
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "nested", "q": "pésti", "sort": ["mother:desc"], "showRankingScore": true },
          {"indexUid" : "nested", "q": "jean-bap", "sort": ["father:desc"], "showRankingScore": true },
          {"indexUid" : "nested", "q": "jea", "sort": ["father:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "id": 654,
          "father": "pierre",
          "mother": "sabine",
          "doggos": [
            {
              "name": "gros bill",
              "age": 8
            }
          ],
          "cattos": [
            "simba",
            "pestiféré"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 0,
            "weightedRankingScore": 0.7803030303030303
          },
          "_rankingScore": 0.7803030303030303
        },
        {
          "id": 852,
          "father": "jean",
          "mother": "michelle",
          "doggos": [
            {
              "name": "bobby",
              "age": 2
            },
            {
              "name": "buddy",
              "age": 4
            }
          ],
          "cattos": "pésti",
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "id": 951,
          "father": "jean-baptiste",
          "mother": "sophie",
          "doggos": [
            {
              "name": "turbo",
              "age": 5
            },
            {
              "name": "fast",
              "age": 6
            }
          ],
          "cattos": [
            "moumoute",
            "gomez"
          ],
          "_federation": {
            "indexUid": "nested",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9991181657848324
          },
          "_rankingScore": 0.9991181657848324
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_same_indexes_different_criterion_opposite_direction() {
    let server = Server::new().await;

    let index = server.index("nested");

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["mother", "father"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // two identical placeholder search should have all results from first query
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "nested", "q": "", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : "nested", "q": "", "sort": ["father:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #0 and #1 are incompatible: \n  1. `queries[0].sort[0]`, `nested.rankingRules[0]`: ascending sort rule(s) on field `mother`\n  2. `queries[1].sort[0]`, `nested.rankingRules[0]`: descending sort rule(s) on field `father`\n  - cannot compare two sort rules in opposite directions\n  - note: The ranking rules of query #0 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n  - note: The ranking rules of query #1 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);

    // mix and match query: should be ranked by ranking score
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "nested", "q": "pésti", "sort": ["mother:asc"], "showRankingScore": true },
          {"indexUid" : "nested", "q": "jean", "sort": ["father:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #0 and #1 are incompatible: \n  1. `queries[0].sort[0]`, `nested.rankingRules[0]`: ascending sort rule(s) on field `mother`\n  2. `queries[1].sort[0]`, `nested.rankingRules[0]`: descending sort rule(s) on field `father`\n  - cannot compare two sort rules in opposite directions\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_different_indexes_same_criterion_same_direction() {
    let server = Server::new().await;

    let index = server.index("movies");

    let documents = DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("batman");

    let documents = SCORE_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // return titles ordered accross indexes
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "movies", "q": "", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : "batman", "q": "", "sort": ["title:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Escape Room",
          "id": "522681",
          "color": [
            "yellow",
            "red"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "id": "166428",
          "color": [
            "green",
            "red"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Shazam!",
          "id": "287947",
          "color": [
            "green",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 10
    }
    "###);

    // mix and match query: will be sorted across indexes
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "batman", "q": "badman returns", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : "movies", "q": "captain", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : "batman", "q": "the bat", "sort": ["title:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          },
          "_rankingScore": 0.9848484848484848
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9528218694885362
          },
          "_rankingScore": 0.9528218694885362
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9528218694885362
          },
          "_rankingScore": 0.9528218694885362
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8317901234567902
          },
          "_rankingScore": 0.8317901234567902
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 0,
            "weightedRankingScore": 0.23106060606060605
          },
          "_rankingScore": 0.23106060606060605
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 0,
            "weightedRankingScore": 0.5
          },
          "_rankingScore": 0.5
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 6
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_different_ranking_rules() {
    let server = Server::new().await;

    let index = server.index("movies");

    let documents = DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("batman");

    let documents = SCORE_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "rankingRules": [
            "words",
            "typo",
            "proximity",
            "attribute",
            "sort",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // return titles ordered accross indexes
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "movies", "q": "", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : "batman", "q": "", "sort": ["title:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Escape Room",
          "id": "522681",
          "color": [
            "yellow",
            "red"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "id": "166428",
          "color": [
            "green",
            "red"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Shazam!",
          "id": "287947",
          "color": [
            "green",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 10
    }
    "###);

    // mix and match query: order difficult to understand
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "batman", "q": "badman returns", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : "movies", "q": "captain", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : "batman", "q": "the bat", "sort": ["title:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #2 and #1 are incompatible: \n  1. `queries[2]`, `batman.rankingRules[0..=3]`: relevancy rule(s) words, typo, proximity, attribute\n  2. `queries[1].sort[0]`, `movies.rankingRules[0]`: descending sort rule(s) on field `title`\n  - cannot compare a relevancy rule with a sort rule\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_different_indexes_same_criterion_opposite_direction() {
    let server = Server::new().await;

    let index = server.index("movies");

    let documents = DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("batman");

    let documents = SCORE_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // all results from query 0
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "movies", "q": "", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : "batman", "q": "", "sort": ["title:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[0]`: The results of queries #1 and #0 are incompatible: \n  1. `queries[1].sort[0]`, `batman.rankingRules[0]`: descending sort rule(s) on field `title`\n  2. `queries[0].sort[0]`, `movies.rankingRules[0]`: ascending sort rule(s) on field `title`\n  - cannot compare two sort rules in opposite directions\n  - note: The ranking rules of query #1 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n  - note: The ranking rules of query #0 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);

    // mix and match query: will be sorted by ranking score
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "batman", "q": "badman returns", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : "movies", "q": "captain", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : "batman", "q": "the bat", "sort": ["title:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #2 and #1 are incompatible: \n  1. `queries[2].sort[0]`, `batman.rankingRules[0]`: ascending sort rule(s) on field `title`\n  2. `queries[1].sort[0]`, `movies.rankingRules[0]`: descending sort rule(s) on field `title`\n  - cannot compare two sort rules in opposite directions\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_different_indexes_different_criterion_same_direction() {
    let server = Server::new().await;

    let index = server.index("movies");

    let documents = DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("batman");

    let documents = SCORE_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["id"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // return titles ordered accross indexes
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "movies", "q": "", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : "batman", "q": "", "sort": ["id:asc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Escape Room",
          "id": "522681",
          "color": [
            "yellow",
            "red"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Gläss",
          "id": "450465",
          "color": [
            "blue",
            "red"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "id": "166428",
          "color": [
            "green",
            "red"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        },
        {
          "title": "Shazam!",
          "id": "287947",
          "color": [
            "green",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          },
          "_rankingScore": 1.0
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 10
    }
    "###);

    // mix and match query: will be sorted across indexes and criterion
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "batman", "q": "badman returns", "sort": ["id:desc"], "showRankingScore": true },
          {"indexUid" : "movies", "q": "captain", "sort": ["title:desc"], "showRankingScore": true },
          {"indexUid" : "batman", "q": "the bat", "sort": ["id:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 0,
            "weightedRankingScore": 0.5
          },
          "_rankingScore": 0.5
        },
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 0,
            "weightedRankingScore": 0.23106060606060605
          },
          "_rankingScore": 0.23106060606060605
        },
        {
          "title": "Captain Marvel",
          "id": "299537",
          "color": [
            "yellow",
            "blue"
          ],
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9848484848484848
          },
          "_rankingScore": 0.9848484848484848
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8317901234567902
          },
          "_rankingScore": 0.8317901234567902
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9528218694885362
          },
          "_rankingScore": 0.9528218694885362
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9528218694885362
          },
          "_rankingScore": 0.9528218694885362
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 6
    }
    "###);
}

#[actix_rt::test]
async fn federation_sort_different_indexes_different_criterion_opposite_direction() {
    let server = Server::new().await;

    let index = server.index("movies");

    let documents = DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("batman");

    let documents = SCORE_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["id"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // all results from query 0 first
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "movies", "q": "", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : "batman", "q": "", "sort": ["id:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[0]`: The results of queries #1 and #0 are incompatible: \n  1. `queries[1].sort[0]`, `batman.rankingRules[0]`: descending sort rule(s) on field `id`\n  2. `queries[0].sort[0]`, `movies.rankingRules[0]`: ascending sort rule(s) on field `title`\n  - cannot compare two sort rules in opposite directions\n  - note: The ranking rules of query #1 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n  - note: The ranking rules of query #0 were modified during canonicalization:\n    1. Removed relevancy rule `words` at position #1 in ranking rules because the query is a placeholder search (`q`: \"\")\n    2. Removed relevancy rule `typo` at position #2 in ranking rules because the query is a placeholder search (`q`: \"\")\n    3. Removed relevancy rule `proximity` at position #3 in ranking rules because the query is a placeholder search (`q`: \"\")\n    4. Removed relevancy rule `attribute` at position #4 in ranking rules because the query is a placeholder search (`q`: \"\")\n    5. Removed relevancy rule `exactness` at position #5 in ranking rules because the query is a placeholder search (`q`: \"\")\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);

    // mix and match query: more or less by ranking score
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "batman", "q": "badman returns", "sort": ["id:desc"], "showRankingScore": true },
          {"indexUid" : "movies", "q": "captain", "sort": ["title:asc"], "showRankingScore": true },
          {"indexUid" : "batman", "q": "the bat", "sort": ["id:desc"], "showRankingScore": true },
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: The results of queries #2 and #1 are incompatible: \n  1. `queries[2].sort[0]`, `batman.rankingRules[0]`: descending sort rule(s) on field `id`\n  2. `queries[1].sort[0]`, `movies.rankingRules[0]`: ascending sort rule(s) on field `title`\n  - cannot compare two sort rules in opposite directions\n",
      "code": "invalid_multi_search_query_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn federation_limit_offset() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("score");
    let documents = SCORE_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();
    {
        let (response, code) = server
            .multi_search(json!({"federation": {}, "queries": [
            {"indexUid" : "test", "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : "test", "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : "test", "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid": "score", "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid": "test", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : "score", "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
        {
          "hits": [
            {
              "title": "Gläss",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 0,
                "weightedRankingScore": 1.0
              }
            },
            {
              "id": 852,
              "_federation": {
                "indexUid": "nested",
                "queriesPosition": 2,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Batman",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Batman Returns",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 10,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Captain Marvel",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 1,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Escape Room",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 3,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 951,
              "_federation": {
                "indexUid": "nested",
                "queriesPosition": 4,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 1",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 2",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 654,
              "_federation": {
                "indexUid": "nested",
                "queriesPosition": 2,
                "weightedRankingScore": 0.7803030303030303
              }
            },
            {
              "title": "Badman",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 8,
                "weightedRankingScore": 0.5
              }
            },
            {
              "title": "How to Train Your Dragon: The Hidden World",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 6,
                "weightedRankingScore": 0.4166666666666667
              }
            }
          ],
          "processingTimeMs": "[time]",
          "limit": 20,
          "offset": 0,
          "estimatedTotalHits": 12
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"limit": 1}, "queries": [
            {"indexUid" : "test", "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : "test", "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : "test", "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid": "score", "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid": "test", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : "score", "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
        {
          "hits": [
            {
              "title": "Gläss",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 0,
                "weightedRankingScore": 1.0
              }
            }
          ],
          "processingTimeMs": "[time]",
          "limit": 1,
          "offset": 0,
          "estimatedTotalHits": 12
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"offset": 2}, "queries": [
            {"indexUid" : "test", "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : "test", "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : "test", "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid": "score", "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid": "test", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : "score", "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
        {
          "hits": [
            {
              "title": "Batman",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Batman Returns",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 10,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Captain Marvel",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 1,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Escape Room",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 3,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 951,
              "_federation": {
                "indexUid": "nested",
                "queriesPosition": 4,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 1",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 2",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 654,
              "_federation": {
                "indexUid": "nested",
                "queriesPosition": 2,
                "weightedRankingScore": 0.7803030303030303
              }
            },
            {
              "title": "Badman",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 8,
                "weightedRankingScore": 0.5
              }
            },
            {
              "title": "How to Train Your Dragon: The Hidden World",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 6,
                "weightedRankingScore": 0.4166666666666667
              }
            }
          ],
          "processingTimeMs": "[time]",
          "limit": 20,
          "offset": 2,
          "estimatedTotalHits": 12
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"offset": 12}, "queries": [
            {"indexUid" : "test", "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : "test", "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : "test", "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid": "score", "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid": "test", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : "score", "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
        {
          "hits": [],
          "processingTimeMs": "[time]",
          "limit": 20,
          "offset": 12,
          "estimatedTotalHits": 12
        }
        "###);
    }
}

#[actix_rt::test]
async fn federation_formatting() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("nested");
    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let index = server.index("score");
    let documents = SCORE_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();
    {
        let (response, code) = server
            .multi_search(json!({"federation": {}, "queries": [
            {"indexUid" : "test", "q": "glass", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid" : "test", "q": "captain", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid": "nested", "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : "test", "q": "Escape", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid": "nested", "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid": "score", "q": "jean", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid": "test", "q": "the bat", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid": "score", "q": "the bat", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid": "score", "q": "badman returns", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid" : "score", "q": "batman", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            {"indexUid": "score", "q": "batman returns", "attributesToRetrieve": ["title"], "attributesToHighlight": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
        {
          "hits": [
            {
              "title": "Gläss",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 0,
                "weightedRankingScore": 1.0
              },
              "_formatted": {
                "title": "<em>Gläss</em>"
              }
            },
            {
              "id": 852,
              "_federation": {
                "indexUid": "nested",
                "queriesPosition": 2,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Batman",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 1.0
              },
              "_formatted": {
                "title": "<em>Batman</em>"
              }
            },
            {
              "title": "Batman Returns",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 10,
                "weightedRankingScore": 1.0
              },
              "_formatted": {
                "title": "<em>Batman</em> <em>Returns</em>"
              }
            },
            {
              "title": "Captain Marvel",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 1,
                "weightedRankingScore": 0.9848484848484848
              },
              "_formatted": {
                "title": "<em>Captain</em> Marvel"
              }
            },
            {
              "title": "Escape Room",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 3,
                "weightedRankingScore": 0.9848484848484848
              },
              "_formatted": {
                "title": "<em>Escape</em> Room"
              }
            },
            {
              "id": 951,
              "_federation": {
                "indexUid": "nested",
                "queriesPosition": 4,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 1",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              },
              "_formatted": {
                "title": "<em>Batman</em> the dark knight returns: Part 1"
              }
            },
            {
              "title": "Batman the dark knight returns: Part 2",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              },
              "_formatted": {
                "title": "<em>Batman</em> the dark knight returns: Part 2"
              }
            },
            {
              "id": 654,
              "_federation": {
                "indexUid": "nested",
                "queriesPosition": 2,
                "weightedRankingScore": 0.7803030303030303
              }
            },
            {
              "title": "Badman",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 8,
                "weightedRankingScore": 0.5
              },
              "_formatted": {
                "title": "<em>Badman</em>"
              }
            },
            {
              "title": "How to Train Your Dragon: The Hidden World",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 6,
                "weightedRankingScore": 0.4166666666666667
              },
              "_formatted": {
                "title": "How to Train Your Dragon: <em>The</em> Hidden World"
              }
            }
          ],
          "processingTimeMs": "[time]",
          "limit": 20,
          "offset": 0,
          "estimatedTotalHits": 12
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"limit": 1}, "queries": [
            {"indexUid" : "test", "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : "test", "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : "test", "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid": "score", "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid": "test", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : "score", "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
        {
          "hits": [
            {
              "title": "Gläss",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 0,
                "weightedRankingScore": 1.0
              }
            }
          ],
          "processingTimeMs": "[time]",
          "limit": 1,
          "offset": 0,
          "estimatedTotalHits": 12
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"offset": 2}, "queries": [
            {"indexUid" : "test", "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : "test", "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : "test", "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid": "score", "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid": "test", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : "score", "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
        {
          "hits": [
            {
              "title": "Batman",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Batman Returns",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 10,
                "weightedRankingScore": 1.0
              }
            },
            {
              "title": "Captain Marvel",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 1,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Escape Room",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 3,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 951,
              "_federation": {
                "indexUid": "nested",
                "queriesPosition": 4,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 1",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "title": "Batman the dark knight returns: Part 2",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 9,
                "weightedRankingScore": 0.9848484848484848
              }
            },
            {
              "id": 654,
              "_federation": {
                "indexUid": "nested",
                "queriesPosition": 2,
                "weightedRankingScore": 0.7803030303030303
              }
            },
            {
              "title": "Badman",
              "_federation": {
                "indexUid": "score",
                "queriesPosition": 8,
                "weightedRankingScore": 0.5
              }
            },
            {
              "title": "How to Train Your Dragon: The Hidden World",
              "_federation": {
                "indexUid": "test",
                "queriesPosition": 6,
                "weightedRankingScore": 0.4166666666666667
              }
            }
          ],
          "processingTimeMs": "[time]",
          "limit": 20,
          "offset": 2,
          "estimatedTotalHits": 12
        }
        "###);
    }

    {
        let (response, code) = server
            .multi_search(json!({"federation": {"offset": 12}, "queries": [
            {"indexUid" : "test", "q": "glass", "attributesToRetrieve": ["title"]},
            {"indexUid" : "test", "q": "captain", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "pésti", "attributesToRetrieve": ["id"]},
            {"indexUid" : "test", "q": "Escape", "attributesToRetrieve": ["title"]},
            {"indexUid": "nested", "q": "jean", "attributesToRetrieve": ["id"]},
            {"indexUid": "score", "q": "jean", "attributesToRetrieve": ["title"]},
            {"indexUid": "test", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "the bat", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "badman returns", "attributesToRetrieve": ["title"]},
            {"indexUid" : "score", "q": "batman", "attributesToRetrieve": ["title"]},
            {"indexUid": "score", "q": "batman returns", "attributesToRetrieve": ["title"]},
            ]}))
            .await;
        snapshot!(code, @"200 OK");
        insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
        {
          "hits": [],
          "processingTimeMs": "[time]",
          "limit": 20,
          "offset": 12,
          "estimatedTotalHits": 12
        }
        "###);
    }
}

#[actix_rt::test]
async fn federation_invalid_weight() {
    let server = Server::new().await;

    let index = server.index("fruits");

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST"]}),
        )
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "fruits", "q": "apple red", "filter": "BOOST = true", "showRankingScore": true, "federationOptions": {"weight": 3.0}},
        {"indexUid": "fruits", "q": "apple red", "showRankingScore": true, "federationOptions": {"weight": -12}},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Invalid value at `.queries[1].federationOptions.weight`: the value of `weight` is invalid, expected a positive float (>= 0.0).",
      "code": "invalid_multi_search_weight",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_weight"
    }
    "###);
}

#[actix_rt::test]
async fn federation_null_weight() {
    let server = Server::new().await;

    let index = server.index("fruits");

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST"]}),
        )
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "fruits", "q": "apple red", "filter": "BOOST = true", "showRankingScore": true, "federationOptions": {"weight": 3.0}},
        {"indexUid": "fruits", "q": "apple red", "showRankingScore": true, "federationOptions": {"weight": 0.0} },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "name": "Exclusive sale: Red delicious apple",
          "id": "red-delicious-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "fruits",
            "queriesPosition": 0,
            "weightedRankingScore": 2.7281746031746033
          },
          "_rankingScore": 0.9093915343915344
        },
        {
          "name": "Exclusive sale: green apple",
          "id": "green-apple-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "fruits",
            "queriesPosition": 0,
            "weightedRankingScore": 1.318181818181818
          },
          "_rankingScore": 0.4393939393939394
        },
        {
          "name": "Red apple gala",
          "id": "red-apple-gala",
          "_federation": {
            "indexUid": "fruits",
            "queriesPosition": 1,
            "weightedRankingScore": 0.0
          },
          "_rankingScore": 0.953042328042328
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3
    }
    "###);
}

#[actix_rt::test]
async fn federation_federated_contains_pagination() {
    let server = Server::new().await;

    let index = server.index("fruits");

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    // fail when a federated query contains "limit"
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits", "q": "apple red", "limit": 5},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: Using pagination options is not allowed in federated queries.\n - Hint: remove `limit` from query #1 or remove `federation` from the request\n - Hint: pass `federation.limit` and `federation.offset` for pagination in federated search",
      "code": "invalid_multi_search_query_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_pagination"
    }
    "###);
    // fail when a federated query contains "offset"
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits", "q": "apple red", "offset": 5},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: Using pagination options is not allowed in federated queries.\n - Hint: remove `offset` from query #1 or remove `federation` from the request\n - Hint: pass `federation.limit` and `federation.offset` for pagination in federated search",
      "code": "invalid_multi_search_query_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_pagination"
    }
    "###);
    // fail when a federated query contains "page"
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits", "q": "apple red", "page": 2},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: Using pagination options is not allowed in federated queries.\n - Hint: remove `page` from query #1 or remove `federation` from the request\n - Hint: pass `federation.limit` and `federation.offset` for pagination in federated search",
      "code": "invalid_multi_search_query_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_pagination"
    }
    "###);
    // fail when a federated query contains "hitsPerPage"
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits", "q": "apple red", "hitsPerPage": 5},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: Using pagination options is not allowed in federated queries.\n - Hint: remove `hitsPerPage` from query #1 or remove `federation` from the request\n - Hint: pass `federation.limit` and `federation.offset` for pagination in federated search",
      "code": "invalid_multi_search_query_pagination",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_pagination"
    }
    "###);
}

#[actix_rt::test]
async fn federation_federated_contains_facets() {
    let server = Server::new().await;

    let index = server.index("fruits");

    let (value, _) = index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST"]}),
        )
        .await;

    index.wait_task(value.uid()).await.succeeded();

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    // empty facets are actually OK
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits", "q": "apple red", "facets": []},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "name": "Red apple gala",
          "id": "red-apple-gala",
          "_federation": {
            "indexUid": "fruits",
            "queriesPosition": 0,
            "weightedRankingScore": 0.953042328042328
          }
        },
        {
          "name": "Exclusive sale: Red delicious apple",
          "id": "red-delicious-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "fruits",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9093915343915344
          }
        },
        {
          "name": "Exclusive sale: green apple",
          "id": "green-apple-boosted",
          "BOOST": true,
          "_federation": {
            "indexUid": "fruits",
            "queriesPosition": 0,
            "weightedRankingScore": 0.4393939393939394
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3
    }
    "###);

    // fails
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits", "q": "apple red", "facets": ["BOOSTED"]},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: Using facet options is not allowed in federated queries.\n - Hint: remove `facets` from query #1 or remove `federation` from the request\n - Hint: pass `federation.facetsByIndex.fruits: [\"BOOSTED\"]` for facets in federated search",
      "code": "invalid_multi_search_query_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_query_facets"
    }
    "###);
}

#[actix_rt::test]
async fn federation_non_faceted_for_an_index() {
    let server = Server::new().await;

    let index = server.index("fruits");

    let (value, _) = index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST", "id", "name"]}),
        )
        .await;

    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("fruits-no-name");

    let (value, _) = index
        .update_settings(
            json!({"searchableAttributes": ["name"], "filterableAttributes": ["BOOST", "id"]}),
        )
        .await;

    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("fruits-no-facets");

    let (value, _) = index.update_settings(json!({"searchableAttributes": ["name"]})).await;

    index.wait_task(value.uid()).await.succeeded();

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    // fails
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            "fruits": ["BOOST", "id", "name"],
            "fruits-no-name": ["BOOST", "id", "name"],
          }
        }, "queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits-no-name", "q": "apple red"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.federation.facetsByIndex.fruits-no-name`: Invalid facet distribution, attribute `name` is not filterable. The available filterable attributes are `BOOST, id`.\n - Note: index `fruits-no-name` used in `.queries[1]`",
      "code": "invalid_multi_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_facets"
    }
    "###);

    // still fails
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            "fruits": ["BOOST", "id", "name"],
            "fruits-no-name": ["BOOST", "id", "name"],
          }
        }, "queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits", "q": "apple red"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.federation.facetsByIndex.fruits-no-name`: Invalid facet distribution, attribute `name` is not filterable. The available filterable attributes are `BOOST, id`.\n - Note: index `fruits-no-name` is not used in queries",
      "code": "invalid_multi_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_facets"
    }
    "###);

    // fails
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            "fruits": ["BOOST", "id", "name"],
            "fruits-no-name": ["BOOST", "id"],
            "fruits-no-facets": ["BOOST", "id"],
          }
        }, "queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits", "q": "apple red"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.federation.facetsByIndex.fruits-no-facets`: Invalid facet distribution, this index does not have configured filterable attributes.\n - Note: index `fruits-no-facets` is not used in queries",
      "code": "invalid_multi_search_facets",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_facets"
    }
    "###);

    // also fails
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            "zorglub": ["BOOST", "id", "name"],
            "fruits": ["BOOST", "id", "name"],
          }
        }, "queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits", "q": "apple red"},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.federation.facetsByIndex.zorglub`: Index `zorglub` not found.\n - Note: index `zorglub` is not used in queries",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);
}

#[actix_rt::test]
async fn federation_non_federated_contains_federation_option() {
    let server = Server::new().await;

    let index = server.index("fruits");

    let documents = FRUITS_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    // fail when a non-federated query contains "federationOptions"
    let (response, code) = server
        .multi_search(json!({"queries": [
        {"indexUid" : "fruits", "q": "apple red"},
        {"indexUid": "fruits", "q": "apple red", "federationOptions": {}},
        ]}))
        .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.queries[1]`: Using `federationOptions` is not allowed in a non-federated search.\n - Hint: remove `federationOptions` from query #1 or add `federation` to the request.",
      "code": "invalid_multi_search_federation_options",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_federation_options"
    }
    "###);
}

#[actix_rt::test]
async fn federation_vector_single_index() {
    let server = Server::new().await;

    let index = server.index("vectors");

    let (value, _) = index
        .update_settings(json!({"embedders": {
          "animal": {
            "source": "userProvided",
            "dimensions": 3
          },
          "sentiment": {
            "source": "userProvided",
            "dimensions": 2
          }
        }}))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let documents = VECTOR_DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    // same embedder
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "vectors", "vector": [1.0, 0.0, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}},
        {"indexUid": "vectors", "vector": [0.5, 0.5, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    {
      "hits": [
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9870882034301758
          }
        },
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9728479385375975
          }
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9701486229896544
          }
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9191691875457764
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "semanticHitCount": 4
    }
    "###);

    // distinct embedder
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "vectors", "vector": [1.0, 0.0, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}},
        // joyful and energetic first
        {"indexUid": "vectors", "vector": [0.8, 0.6], "hybrid": {"semanticRatio": 1.0, "embedder": "sentiment"}},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    {
      "hits": [
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 1,
            "weightedRankingScore": 0.979868710041046
          }
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9701486229896544
          }
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8601469993591309
          }
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8432406187057495
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "semanticHitCount": 4
    }
    "###);

    // hybrid search, distinct embedder
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "vectors", "vector": [1.0, 0.0, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}, "showRankingScore": true},
          // joyful and energetic first
          {"indexUid": "vectors", "vector": [0.8, 0.6], "q": "beagle", "hybrid": {"semanticRatio": 1.0, "embedder": "sentiment"},"showRankingScore": true},
          {"indexUid": "vectors", "q": "dog", "showRankingScore": true},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    {
      "hits": [
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 1,
            "weightedRankingScore": 0.979868710041046
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9701486229896544
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9242424242424242
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_federation": {
            "indexUid": "vectors",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8601469993591309
          },
          "_rankingScore": "[score]"
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "semanticHitCount": 3
    }
    "###);
}

#[actix_rt::test]
async fn federation_vector_two_indexes() {
    let server = Server::new().await;

    let index = server.index("vectors-animal");

    let (value, _) = index
        .update_settings(json!({"embedders": {
          "animal": {
            "source": "userProvided",
            "dimensions": 3
          },
        }}))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let documents = VECTOR_DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("vectors-sentiment");

    let (value, _) = index
        .update_settings(json!({"embedders": {
          "sentiment": {
            "source": "userProvided",
            "dimensions": 2
          }
        }}))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let documents = VECTOR_DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
        {"indexUid" : "vectors-animal", "vector": [1.0, 0.0, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}, "retrieveVectors": true},
        // joyful and energetic first
        {"indexUid": "vectors-sentiment", "vector": [0.8, 0.6], "hybrid": {"semanticRatio": 1.0, "embedder": "sentiment"}, "retrieveVectors": true},
        {"indexUid": "vectors-sentiment", "q": "dog", "retrieveVectors": true},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    {
      "hits": [
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_vectors": {
            "animal": [
              0.8,
              0.09,
              0.8
            ],
            "sentiment": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.30000001192092896
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment",
            "queriesPosition": 1,
            "weightedRankingScore": 0.979868710041046
          }
        },
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_vectors": {
            "sentiment": [
              0.8,
              0.3
            ],
            "animal": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.09000000357627869,
                  0.800000011920929
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9728479385375975
          }
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_vectors": {
            "sentiment": [
              -1.0,
              0.1
            ],
            "animal": {
              "embeddings": [
                [
                  0.8500000238418579,
                  0.019999999552965164,
                  0.10000000149011612
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9701486229896544
          }
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_vectors": {
            "animal": [
              0.9,
              0.8,
              0.05
            ],
            "sentiment": {
              "embeddings": [
                [
                  -0.10000000149011612,
                  0.550000011920929
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9242424242424242
          }
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_vectors": {
            "animal": [
              0.85,
              0.02,
              0.1
            ],
            "sentiment": {
              "embeddings": [
                [
                  -1.0,
                  0.10000000149011612
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9242424242424242
          }
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_vectors": {
            "sentiment": [
              -0.2,
              0.65
            ],
            "animal": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.8999999761581421,
                  0.5
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8601469993591309
          }
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_vectors": {
            "sentiment": [
              -0.1,
              0.55
            ],
            "animal": {
              "embeddings": [
                [
                  0.8999999761581421,
                  0.800000011920929,
                  0.05000000074505806
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8432406187057495
          }
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_vectors": {
            "animal": [
              0.8,
              0.9,
              0.5
            ],
            "sentiment": {
              "embeddings": [
                [
                  -0.20000000298023224,
                  0.6499999761581421
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment",
            "queriesPosition": 1,
            "weightedRankingScore": 0.6690993905067444
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 8,
      "semanticHitCount": 6
    }
    "###);

    // hybrid search, distinct embedder
    let (response, code) = server
        .multi_search(json!({"federation": {}, "queries": [
          {"indexUid" : "vectors-animal", "vector": [1.0, 0.0, 0.5], "hybrid": {"semanticRatio": 1.0, "embedder": "animal"}, "showRankingScore": true, "retrieveVectors": true},
          {"indexUid": "vectors-sentiment", "vector": [-1, 0.6], "q": "beagle", "hybrid": {"semanticRatio": 1.0, "embedder": "sentiment"}, "showRankingScore": true, "retrieveVectors": true,},
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]", ".**._rankingScore" => "[score]" }, @r###"
    {
      "hits": [
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_vectors": {
            "sentiment": [
              0.8,
              0.3
            ],
            "animal": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.09000000357627869,
                  0.800000011920929
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9728479385375975
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_vectors": {
            "sentiment": [
              -1.0,
              0.1
            ],
            "animal": {
              "embeddings": [
                [
                  0.8500000238418579,
                  0.019999999552965164,
                  0.10000000149011612
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9701486229896544
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "C",
          "description": "the dog had to stay alone today",
          "_vectors": {
            "animal": [
              0.85,
              0.02,
              0.1
            ],
            "sentiment": {
              "embeddings": [
                [
                  -1.0,
                  0.10000000149011612
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9522157907485962
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_vectors": {
            "animal": [
              0.8,
              0.9,
              0.5
            ],
            "sentiment": {
              "embeddings": [
                [
                  -0.20000000298023224,
                  0.6499999761581421
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8719604015350342
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "B",
          "description": "the kitten scratched the beagle",
          "_vectors": {
            "sentiment": [
              -0.2,
              0.65
            ],
            "animal": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.8999999761581421,
                  0.5
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8601469993591309
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_vectors": {
            "sentiment": [
              -0.1,
              0.55
            ],
            "animal": {
              "embeddings": [
                [
                  0.8999999761581421,
                  0.800000011920929,
                  0.05000000074505806
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-animal",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8432406187057495
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "A",
          "description": "the dog barks at the cat",
          "_vectors": {
            "animal": [
              0.9,
              0.8,
              0.05
            ],
            "sentiment": {
              "embeddings": [
                [
                  -0.10000000149011612,
                  0.550000011920929
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8297949433326721
          },
          "_rankingScore": "[score]"
        },
        {
          "id": "D",
          "description": "the little boy pets the puppy",
          "_vectors": {
            "animal": [
              0.8,
              0.09,
              0.8
            ],
            "sentiment": {
              "embeddings": [
                [
                  0.800000011920929,
                  0.30000001192092896
                ]
              ],
              "regenerate": false
            }
          },
          "_federation": {
            "indexUid": "vectors-sentiment",
            "queriesPosition": 1,
            "weightedRankingScore": 0.18887794017791748
          },
          "_rankingScore": "[score]"
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 8,
      "semanticHitCount": 8
    }
    "###);
}

#[actix_rt::test]
async fn federation_facets_different_indexes_same_facet() {
    let server = Server::new().await;

    let index = server.index("movies");

    let documents = DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "filterableAttributes": ["title", "color"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("batman");

    let documents = SCORE_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "filterableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("batman-2");

    let documents = SCORE_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "filterableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // return titles ordered accross indexes
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            "movies": ["title", "color"],
            "batman": ["title"],
            "batman-2": ["title"],
          }
        }, "queries": [
          {"indexUid" : "movies", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
          {"indexUid" : "batman", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
          {"indexUid" : "batman-2", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 15,
      "facetsByIndex": {
        "batman": {
          "distribution": {
            "title": {
              "Badman": 1,
              "Batman": 1,
              "Batman Returns": 1,
              "Batman the dark knight returns: Part 1": 1,
              "Batman the dark knight returns: Part 2": 1
            }
          },
          "stats": {}
        },
        "batman-2": {
          "distribution": {
            "title": {
              "Badman": 1,
              "Batman": 1,
              "Batman Returns": 1,
              "Batman the dark knight returns: Part 1": 1,
              "Batman the dark knight returns: Part 2": 1
            }
          },
          "stats": {}
        },
        "movies": {
          "distribution": {
            "color": {
              "blue": 3,
              "green": 2,
              "red": 3,
              "yellow": 2
            },
            "title": {
              "Captain Marvel": 1,
              "Escape Room": 1,
              "Gläss": 1,
              "How to Train Your Dragon: The Hidden World": 1,
              "Shazam!": 1
            }
          },
          "stats": {}
        }
      }
    }
    "###);

    let (response, code) = server
    .multi_search(json!({"federation": {
      "facetsByIndex": {
        "movies": ["title"],
        "batman": ["title"],
        "batman-2": ["title"]
      },
      "mergeFacets": {}
    }, "queries": [
      {"indexUid" : "movies", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
      {"indexUid" : "batman", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
      {"indexUid" : "batman-2", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
    ]}))
    .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 15,
      "facetDistribution": {
        "title": {
          "Badman": 2,
          "Batman": 2,
          "Batman Returns": 2,
          "Batman the dark knight returns: Part 1": 2,
          "Batman the dark knight returns: Part 2": 2,
          "Captain Marvel": 1,
          "Escape Room": 1,
          "Gläss": 1,
          "How to Train Your Dragon: The Hidden World": 1,
          "Shazam!": 1
        }
      },
      "facetStats": {}
    }
    "###);

    // mix and match query: will be sorted across indexes
    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            "movies": [],
            "batman": ["title"],
            "batman-2": ["title"]
          }
        }, "queries": [
          {"indexUid" : "batman", "q": "badman returns", "sort": ["title:desc"], "attributesToRetrieve": ["title"] },
          {"indexUid" : "batman-2", "q": "badman returns", "sort": ["title:desc"], "attributesToRetrieve": ["title"] },
          {"indexUid" : "movies", "q": "captain", "sort": ["title:desc"], "attributesToRetrieve": ["title"] },
          {"indexUid" : "batman", "q": "the bat", "sort": ["title:desc"], "attributesToRetrieve": ["title"] },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 3,
            "weightedRankingScore": 0.9528218694885362
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7028218694885362
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 3,
            "weightedRankingScore": 0.9528218694885362
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 1,
            "weightedRankingScore": 0.7028218694885362
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 0,
            "weightedRankingScore": 0.8317901234567902
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 1,
            "weightedRankingScore": 0.8317901234567902
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 0,
            "weightedRankingScore": 0.23106060606060605
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 1,
            "weightedRankingScore": 0.23106060606060605
          }
        },
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 0,
            "weightedRankingScore": 0.5
          }
        },
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman-2",
            "queriesPosition": 1,
            "weightedRankingScore": 0.5
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 11,
      "facetsByIndex": {
        "batman": {
          "distribution": {
            "title": {
              "Badman": 1,
              "Batman": 1,
              "Batman Returns": 1,
              "Batman the dark knight returns: Part 1": 1,
              "Batman the dark knight returns: Part 2": 1
            }
          },
          "stats": {}
        },
        "batman-2": {
          "distribution": {
            "title": {
              "Badman": 1,
              "Batman": 1,
              "Batman Returns": 1,
              "Batman the dark knight returns: Part 1": 1,
              "Batman the dark knight returns: Part 2": 1
            }
          },
          "stats": {}
        },
        "movies": {
          "distribution": {},
          "stats": {}
        }
      }
    }
    "###);
}

#[actix_rt::test]
async fn federation_facets_same_indexes() {
    let server = Server::new().await;

    let index = server.index("doggos");

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "filterableAttributes": ["father", "mother", "doggos.age"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("doggos-2");

    let documents = NESTED_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "filterableAttributes": ["father", "mother", "doggos.age"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            "doggos": ["father", "mother", "doggos.age"]
          }
        }, "queries": [
          {"indexUid" : "doggos", "q": "je", "attributesToRetrieve": ["id"] },
          {"indexUid" : "doggos", "q": "michel", "attributesToRetrieve": ["id"] },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "id": 852,
          "_federation": {
            "indexUid": "doggos",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 951,
          "_federation": {
            "indexUid": "doggos",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 750,
          "_federation": {
            "indexUid": "doggos",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9621212121212122
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 3,
      "facetsByIndex": {
        "doggos": {
          "distribution": {
            "doggos.age": {
              "2": 1,
              "4": 1,
              "5": 1,
              "6": 1
            },
            "father": {
              "jean": 1,
              "jean-baptiste": 1,
              "romain": 1
            },
            "mother": {
              "michelle": 2,
              "sophie": 1
            }
          },
          "stats": {
            "doggos.age": {
              "min": 2.0,
              "max": 6.0
            }
          }
        }
      }
    }
    "###);

    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            "doggos": ["father", "mother", "doggos.age"],
            "doggos-2": ["father", "mother", "doggos.age"]
          }
        }, "queries": [
          {"indexUid" : "doggos", "q": "je", "attributesToRetrieve": ["id"] },
          {"indexUid" : "doggos-2", "q": "michel", "attributesToRetrieve": ["id"] },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "id": 852,
          "_federation": {
            "indexUid": "doggos",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 951,
          "_federation": {
            "indexUid": "doggos",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 852,
          "_federation": {
            "indexUid": "doggos-2",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 750,
          "_federation": {
            "indexUid": "doggos-2",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9621212121212122
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "facetsByIndex": {
        "doggos": {
          "distribution": {
            "doggos.age": {
              "2": 1,
              "4": 1,
              "5": 1,
              "6": 1
            },
            "father": {
              "jean": 1,
              "jean-baptiste": 1
            },
            "mother": {
              "michelle": 1,
              "sophie": 1
            }
          },
          "stats": {
            "doggos.age": {
              "min": 2.0,
              "max": 6.0
            }
          }
        },
        "doggos-2": {
          "distribution": {
            "doggos.age": {
              "2": 1,
              "4": 1
            },
            "father": {
              "jean": 1,
              "romain": 1
            },
            "mother": {
              "michelle": 2
            }
          },
          "stats": {
            "doggos.age": {
              "min": 2.0,
              "max": 4.0
            }
          }
        }
      }
    }
    "###);

    let (response, code) = server
        .multi_search(json!({"federation": {
          "facetsByIndex": {
            "doggos": ["father", "mother", "doggos.age"],
            "doggos-2": ["father", "mother", "doggos.age"]
          },
          "mergeFacets": {},
        }, "queries": [
          {"indexUid" : "doggos", "q": "je", "attributesToRetrieve": ["id"] },
          {"indexUid" : "doggos-2", "q": "michel", "attributesToRetrieve": ["id"] },
        ]}))
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "id": 852,
          "_federation": {
            "indexUid": "doggos",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 951,
          "_federation": {
            "indexUid": "doggos",
            "queriesPosition": 0,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 852,
          "_federation": {
            "indexUid": "doggos-2",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9621212121212122
          }
        },
        {
          "id": 750,
          "_federation": {
            "indexUid": "doggos-2",
            "queriesPosition": 1,
            "weightedRankingScore": 0.9621212121212122
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4,
      "facetDistribution": {
        "doggos.age": {
          "2": 2,
          "4": 2,
          "5": 1,
          "6": 1
        },
        "father": {
          "jean": 2,
          "jean-baptiste": 1,
          "romain": 1
        },
        "mother": {
          "michelle": 3,
          "sophie": 1
        }
      },
      "facetStats": {
        "doggos.age": {
          "min": 2.0,
          "max": 6.0
        }
      }
    }
    "###);
}

#[actix_rt::test]
async fn federation_inconsistent_merge_order() {
    let server = Server::new().await;

    let index = server.index("movies");

    let documents = DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "filterableAttributes": ["title", "color"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("movies-2");

    let documents = DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "filterableAttributes": ["title", "color"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ],
          "faceting": {
            "sortFacetValuesBy": { "color": "count" }
          }
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    let index = server.index("batman");

    let documents = SCORE_DOCUMENTS.clone();
    let (value, _) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.succeeded();

    let (value, _) = index
        .update_settings(json!({
          "sortableAttributes": ["title"],
          "filterableAttributes": ["title"],
          "rankingRules": [
            "sort",
            "words",
            "typo",
            "proximity",
            "attribute",
            "exactness"
          ]
        }))
        .await;
    index.wait_task(value.uid()).await.succeeded();

    // without merging, it works
    let (response, code) = server
      .multi_search(json!({"federation": {
        "facetsByIndex": {
          "movies": ["title", "color"],
          "batman": ["title"],
          "movies-2": ["title", "color"],
        }
      }, "queries": [
        {"indexUid" : "movies", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
        {"indexUid" : "batman", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
        {"indexUid" : "movies-2", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
      ]}))
      .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 15,
      "facetsByIndex": {
        "batman": {
          "distribution": {
            "title": {
              "Badman": 1,
              "Batman": 1,
              "Batman Returns": 1,
              "Batman the dark knight returns: Part 1": 1,
              "Batman the dark knight returns: Part 2": 1
            }
          },
          "stats": {}
        },
        "movies": {
          "distribution": {
            "color": {
              "blue": 3,
              "green": 2,
              "red": 3,
              "yellow": 2
            },
            "title": {
              "Captain Marvel": 1,
              "Escape Room": 1,
              "Gläss": 1,
              "How to Train Your Dragon: The Hidden World": 1,
              "Shazam!": 1
            }
          },
          "stats": {}
        },
        "movies-2": {
          "distribution": {
            "color": {
              "red": 3,
              "blue": 3,
              "yellow": 2,
              "green": 2
            },
            "title": {
              "Captain Marvel": 1,
              "Escape Room": 1,
              "Gläss": 1,
              "How to Train Your Dragon: The Hidden World": 1,
              "Shazam!": 1
            }
          },
          "stats": {}
        }
      }
    }
    "###);

    // fails with merging
    let (response, code) = server
  .multi_search(json!({"federation": {
    "facetsByIndex": {
      "movies": ["title", "color"],
      "batman": ["title"],
      "movies-2": ["title", "color"],
    },
    "mergeFacets": {}
  }, "queries": [
    {"indexUid" : "movies", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
    {"indexUid" : "batman", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
    {"indexUid" : "movies-2", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
  ]}))
  .await;
    snapshot!(code, @"400 Bad Request");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "message": "Inside `.federation.facetsByIndex.movies-2`: Inconsistent order for values in facet `color`: index `movies` orders alphabetically, but index `movies-2` orders by count.\n - Hint: Remove `federation.mergeFacets` or change `faceting.sortFacetValuesBy` to be consistent in settings.\n - Note: index `movies-2` used in `.queries[2]`",
      "code": "invalid_multi_search_facet_order",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_multi_search_facet_order"
    }
    "###);

    // can limit the number of values
    let (response, code) = server
 .multi_search(json!({"federation": {
   "facetsByIndex": {
     "movies": ["title", "color"],
     "batman": ["title"],
     "movies-2": ["title"],
   },
   "mergeFacets": {
     "maxValuesPerFacet": 3,
   }
 }, "queries": [
   {"indexUid" : "movies", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
   {"indexUid" : "batman", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
   {"indexUid" : "movies-2", "q": "", "sort": ["title:asc"], "attributesToRetrieve": ["title"] },
 ]}))
 .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response, { ".processingTimeMs" => "[time]" }, @r###"
    {
      "hits": [
        {
          "title": "Badman",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "_federation": {
            "indexUid": "batman",
            "queriesPosition": 1,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Captain Marvel",
          "_federation": {
            "indexUid": "movies-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Escape Room",
          "_federation": {
            "indexUid": "movies-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Gläss",
          "_federation": {
            "indexUid": "movies-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "How to Train Your Dragon: The Hidden World",
          "_federation": {
            "indexUid": "movies-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies",
            "queriesPosition": 0,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Shazam!",
          "_federation": {
            "indexUid": "movies-2",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        }
      ],
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 15,
      "facetDistribution": {
        "color": {
          "blue": 3,
          "green": 2,
          "red": 3
        },
        "title": {
          "Badman": 1,
          "Batman": 1,
          "Batman Returns": 1
        }
      },
      "facetStats": {}
    }
    "###);
}
