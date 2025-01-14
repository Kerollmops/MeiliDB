use meili_snap::*;

use super::DOCUMENTS;
use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn similar_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");

    let expected_response = json!({
        "message": "Index `test` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    index
        .similar(json!({"id": 287947, "embedder": "manual"}), |response, code| {
            assert_eq!(code, 404);
            assert_eq!(response, expected_response);
        })
        .await;
}

#[actix_rt::test]
async fn similar_unexisting_parameter() {
    let server = Server::new().await;
    let index = server.index("test");

    index
        .similar(json!({"id": 287947, "marin": "hello"}), |response, code| {
            assert_eq!(code, 400, "{}", response);
            assert_eq!(response["code"], "bad_request");
        })
        .await;
}

#[actix_rt::test]
async fn similar_bad_id() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let (response, code) = index.similar_post(json!({"id": ["doggo"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.id`: the value of `id` is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_), and can not be more than 511 bytes.",
      "code": "invalid_similar_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_id"
    }
    "###);
}

#[actix_rt::test]
async fn similar_bad_ranking_score_threshold() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let (response, code) = index.similar_post(json!({"rankingScoreThreshold": ["doggo"]})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.rankingScoreThreshold`: expected a number, but found an array: `[\"doggo\"]`",
      "code": "invalid_similar_ranking_score_threshold",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_ranking_score_threshold"
    }
    "###);
}

#[actix_rt::test]
async fn similar_invalid_ranking_score_threshold() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let (response, code) = index.similar_post(json!({"rankingScoreThreshold": 42})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.rankingScoreThreshold`: the value of `rankingScoreThreshold` is invalid, expected a float between `0.0` and `1.0`.",
      "code": "invalid_similar_ranking_score_threshold",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_ranking_score_threshold"
    }
    "###);
}

#[actix_rt::test]
async fn similar_invalid_id() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let (response, code) = index.similar_post(json!({"id": "http://invalid-docid/"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value at `.id`: the value of `id` is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_), and can not be more than 511 bytes.",
      "code": "invalid_similar_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_id"
    }
    "###);
}

#[actix_rt::test]
async fn similar_not_found_id() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let (response, code) =
        index.similar_post(json!({"id": "definitely-doesnt-exist", "embedder": "manual"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Document `definitely-doesnt-exist` not found.",
      "code": "not_found_similar_id",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#not_found_similar_id"
    }
    "###);
}

#[actix_rt::test]
async fn similar_bad_offset() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let (response, code) =
        index.similar_post(json!({"id": 287947, "offset": "doggo", "embedder": "manual"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.offset`: expected a positive integer, but found a string: `\"doggo\"`",
      "code": "invalid_similar_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_offset"
    }
    "###);

    let (response, code) = index.similar_get("?id=287947&offset=doggo&embedder=manual").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `doggo` as a positive integer",
      "code": "invalid_similar_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_offset"
    }
    "###);
}

#[actix_rt::test]
async fn similar_bad_limit() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let (response, code) =
        index.similar_post(json!({"id": 287947, "limit": "doggo", "embedder": "manual"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.limit`: expected a positive integer, but found a string: `\"doggo\"`",
      "code": "invalid_similar_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_limit"
    }
    "###);

    let (response, code) = index.similar_get("?id=287946&limit=doggo&embedder=manual").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `doggo` as a positive integer",
      "code": "invalid_similar_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_limit"
    }
    "###);
}

#[actix_rt::test]
async fn similar_bad_filter() {
    // Since a filter is deserialized as a json Value it will never fail to deserialize.
    // Thus the error message is not generated by deserr but written by us.
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let (response, code) =
        index.similar_post(json!({ "id": 287947, "filter": true, "embedder": "manual" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid syntax for the filter parameter: `expected String, Array, found: true`.",
      "code": "invalid_similar_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
    }
    "###);
    // Can't make the `filter` fail with a get search since it'll accept anything as a strings.
}

#[actix_rt::test]
async fn filter_invalid_syntax_object() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    index
        .similar(json!({"id": 287947, "filter": "title & Glass", "embedder": "manual"}), |response, code| {
            snapshot!(response, @r###"
            {
              "message": "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `title & Glass`.\n1:14 title & Glass",
              "code": "invalid_similar_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
            }
            "###);
            snapshot!(code, @"400 Bad Request");
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_syntax_array() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    index
        .similar(json!({"id": 287947, "filter": ["title & Glass"], "embedder": "manual"}), |response, code| {
            snapshot!(response, @r###"
            {
              "message": "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `STARTS WITH`, `NOT STARTS WITH`, `_geoRadius`, or `_geoBoundingBox` at `title & Glass`.\n1:14 title & Glass",
              "code": "invalid_similar_filter",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
            }
            "###);
            snapshot!(code, @"400 Bad Request");
        })
        .await;
}

#[actix_rt::test]
async fn filter_invalid_syntax_string() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let expected_response = json!({
        "message": "Found unexpected characters at the end of the filter: `XOR title = Glass`. You probably forgot an `OR` or an `AND` rule.\n15:32 title = Glass XOR title = Glass",
        "code": "invalid_similar_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
    });
    index
        .similar(
            json!({"id": 287947, "filter": "title = Glass XOR title = Glass", "embedder": "manual"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter_invalid_attribute_array() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let expected_response = json!({
        "message": "Attribute `many` is not filterable. Available filterable attributes are: `title`.\n1:5 many = Glass",
        "code": "invalid_similar_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
    });
    index
        .similar(
            json!({"id": 287947, "filter": ["many = Glass"], "embedder": "manual"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter_invalid_attribute_string() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let expected_response = json!({
        "message": "Attribute `many` is not filterable. Available filterable attributes are: `title`.\n1:5 many = Glass",
        "code": "invalid_similar_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
    });
    index
        .similar(
            json!({"id": 287947, "filter": "many = Glass", "embedder": "manual"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_attribute_array() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let expected_response = json!({
        "message": "`_geo` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:13 _geo = Glass",
        "code": "invalid_similar_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
    });
    index
        .similar(
            json!({"id": 287947, "filter": ["_geo = Glass"], "embedder": "manual"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_attribute_string() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let expected_response = json!({
        "message": "`_geo` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:13 _geo = Glass",
        "code": "invalid_similar_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
    });
    index
        .similar(
            json!({"id": 287947, "filter": "_geo = Glass", "embedder": "manual"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter_reserved_attribute_array() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let expected_response = json!({
        "message": "`_geoDistance` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:21 _geoDistance = Glass",
        "code": "invalid_similar_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
    });
    index
        .similar(
            json!({"id": 287947, "filter": ["_geoDistance = Glass"], "embedder": "manual"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter_reserved_attribute_string() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let expected_response = json!({
       "message": "`_geoDistance` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:21 _geoDistance = Glass",
        "code": "invalid_similar_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
    });
    index
        .similar(
            json!({"id": 287947, "filter": "_geoDistance = Glass", "embedder": "manual"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_point_array() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let expected_response = json!({
        "message": "`_geoPoint` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:18 _geoPoint = Glass",
        "code": "invalid_similar_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
    });
    index
        .similar(
            json!({"id": 287947, "filter": ["_geoPoint = Glass"], "embedder": "manual"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter_reserved_geo_point_string() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    let expected_response = json!({
       "message": "`_geoPoint` is a reserved keyword and thus can't be used as a filter expression. Use the `_geoRadius(latitude, longitude, distance)` or `_geoBoundingBox([latitude, longitude], [latitude, longitude])` built-in rules to filter on `_geo` coordinates.\n1:18 _geoPoint = Glass",
        "code": "invalid_similar_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_filter"
    });
    index
        .similar(
            json!({"id": 287947, "filter": "_geoPoint = Glass", "embedder": "manual"}),
            |response, code| {
                assert_eq!(response, expected_response);
                assert_eq!(code, 400);
            },
        )
        .await;
}

#[actix_rt::test]
async fn similar_bad_retrieve_vectors() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) =
        index.similar_post(json!({"retrieveVectors": "doggo", "embedder": "manual"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.retrieveVectors`: expected a boolean, but found a string: `\"doggo\"`",
      "code": "invalid_similar_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_retrieve_vectors"
    }
    "###);

    let (response, code) =
        index.similar_post(json!({"retrieveVectors": [true], "embedder": "manual"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `.retrieveVectors`: expected a boolean, but found an array: `[true]`",
      "code": "invalid_similar_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_retrieve_vectors"
    }
    "###);

    let (response, code) = index.similar_get("?retrieveVectors=").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `retrieveVectors`: could not parse `` as a boolean, expected either `true` or `false`",
      "code": "invalid_similar_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_retrieve_vectors"
    }
    "###);

    let (response, code) = index.similar_get("?retrieveVectors=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `retrieveVectors`: could not parse `doggo` as a boolean, expected either `true` or `false`",
      "code": "invalid_similar_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_similar_retrieve_vectors"
    }
    "###);
}

#[actix_rt::test]
async fn similar_bad_embedder() {
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({
        "embedders": {
            "manual": {
                "source": "userProvided",
                "dimensions": 3,
            }
        },
        "filterableAttributes": ["title"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await;

    let expected_response = json!({
    "message": "Cannot find embedder with name `auto`.",
    "code": "invalid_similar_embedder",
    "type": "invalid_request",
    "link": "https://docs.meilisearch.com/errors#invalid_similar_embedder"
    });

    index
        .similar(json!({"id": 287947, "embedder": "auto"}), |response, code| {
            assert_eq!(response, expected_response);
            assert_eq!(code, 400);
        })
        .await;

    let expected_response = json!({
        "message": "Invalid value type at `.embedder`: expected a string, but found a positive integer: `42`",
        "code": "invalid_similar_embedder",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_embedder"
    });

    let (response, code) = index.similar_post(json!({"id": 287947, "embedder": 42})).await;

    assert_eq!(response, expected_response);
    assert_eq!(code, 400);

    let expected_response = json!({
        "message": "Invalid value type at `.embedder`: expected a string, but found null",
        "code": "invalid_similar_embedder",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_similar_embedder"
    });

    let (response, code) = index.similar_post(json!({"id": 287947, "embedder": null})).await;

    assert_eq!(response, expected_response);
    assert_eq!(code, 400);

    let expected_response = json!({
       "message": "Missing field `embedder`",
        "code": "bad_request",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#bad_request"
    });

    let (response, code) = index.similar_post(json!({"id": 287947})).await;
    assert_eq!(response, expected_response);
    assert_eq!(code, 400);

    let expected_response = json!({
       "message": "Missing parameter `embedder`",
        "code": "bad_request",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#bad_request"
    });
    let (response, code) = index.similar_get("?id=287947").await;
    assert_eq!(response, expected_response);
    assert_eq!(code, 400);
}
