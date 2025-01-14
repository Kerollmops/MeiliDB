// This modules contains all the test concerning search. Each particular feature of the search
// should be tested in its own module to isolate tests and keep the tests readable.

mod distinct;
mod errors;
mod facet_search;
mod formatted;
mod geo;
mod hybrid;
#[cfg(not(feature = "chinese-pinyin"))]
mod locales;
mod matching_strategy;
mod multi;
mod pagination;
mod restrict_searchable;
mod search_queue;

use meili_snap::{json_string, snapshot};
use meilisearch::Opt;
use tempfile::TempDir;

use crate::common::{
    default_settings, shared_index_with_documents, shared_index_with_nested_documents, Server,
    DOCUMENTS, FRUITS_DOCUMENTS, NESTED_DOCUMENTS, SCORE_DOCUMENTS, VECTOR_DOCUMENTS,
};
use crate::json;

#[actix_rt::test]
async fn simple_placeholder_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 5);
        })
        .await;

    let index = shared_index_with_nested_documents().await;
    index
        .search(json!({}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 4);
        })
        .await;
}

#[actix_rt::test]
async fn simple_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"q": "glass"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;

    let index = shared_index_with_nested_documents().await;
    index
        .search(json!({"q": "pésti"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 2);
        })
        .await;
}

#[actix_rt::test]
async fn search_with_stop_word() {
    // related to https://github.com/meilisearch/meilisearch/issues/4984
    let server = Server::new().await;
    let index = server.index("test");

    let (_, code) = index
        .update_settings(json!({"stopWords": ["the", "The", "a", "an", "to", "in", "of"]}))
        .await;
    meili_snap::snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    index.add_documents(documents, None).await;
    index.wait_task(1).await;

    // prefix search
    index
        .search(json!({"q": "to the", "attributesToHighlight": ["title"], "attributesToRetrieve": ["title"] }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            snapshot!(json_string!(response["hits"]), @"[]");
        })
        .await;

    // non-prefix search
    index
          .search(json!({"q": "to the ", "attributesToHighlight": ["title"], "attributesToRetrieve": ["title"] }), |response, code| {
              assert_eq!(code, 200, "{}", response);
              snapshot!(json_string!(response["hits"]), @r###"
              [
                {
                  "title": "Shazam!",
                  "_formatted": {
                    "title": "Shazam!"
                  }
                },
                {
                  "title": "Captain Marvel",
                  "_formatted": {
                    "title": "Captain Marvel"
                  }
                },
                {
                  "title": "Escape Room",
                  "_formatted": {
                    "title": "Escape Room"
                  }
                },
                {
                  "title": "How to Train Your Dragon: The Hidden World",
                  "_formatted": {
                    "title": "How to Train Your Dragon: The Hidden World"
                  }
                },
                {
                  "title": "Gläss",
                  "_formatted": {
                    "title": "Gläss"
                  }
                }
              ]
              "###);
          })
          .await;
}

#[actix_rt::test]
async fn phrase_search_with_stop_word() {
    // related to https://github.com/meilisearch/meilisearch/issues/3521
    let server = Server::new().await;
    let index = server.index("test");

    let (_, code) = index.update_settings(json!({"stopWords": ["the", "of"]})).await;
    meili_snap::snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "how \"to\" train \"the" }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;
}

#[actix_rt::test]
async fn negative_phrase_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"q": "-\"train your dragon\"" }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 4);
            assert_eq!(hits[0]["id"], "287947");
            assert_eq!(hits[1]["id"], "299537");
            assert_eq!(hits[2]["id"], "522681");
            assert_eq!(hits[3]["id"], "450465");
        })
        .await;
}

#[actix_rt::test]
async fn negative_word_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"q": "-escape" }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 4);
            assert_eq!(hits[0]["id"], "287947");
            assert_eq!(hits[1]["id"], "299537");
            assert_eq!(hits[2]["id"], "166428");
            assert_eq!(hits[3]["id"], "450465");
        })
        .await;

    // Everything that contains derivates of escape but not escape: nothing
    index
        .search(json!({"q": "-escape escape" }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 0);
        })
        .await;
}

#[actix_rt::test]
async fn non_negative_search() {
    let index = shared_index_with_documents().await;
    index
        .search(json!({"q": "- escape" }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0]["id"], "522681");
        })
        .await;

    index
        .search(json!({"q": "- \"train your dragon\"" }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0]["id"], "166428");
        })
        .await;
}

#[actix_rt::test]
async fn negative_special_cases_search() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (task, _status_code) =
        index.update_settings(json!({"synonyms": { "escape": ["gläss"] }})).await;
    index.wait_task(task.uid()).await.succeeded();

    // There is a synonym for escape -> glass but we don't want "escape", only the derivates: glass
    index
        .search(json!({"q": "-escape escape" }), |response, code| {
            assert_eq!(code, 200, "{}", response);
            let hits = response["hits"].as_array().unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0]["id"], "450465");
        })
        .await;
}

#[cfg(feature = "default")]
#[cfg(not(feature = "chinese-pinyin"))]
#[actix_rt::test]
async fn test_kanji_language_detection() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        { "id": 0, "title": "The quick (\"brown\") fox can't jump 32.3 feet, right? Brr, it's 29.3°F!" },
        { "id": 1, "title": "東京のお寿司。" },
        { "id": 2, "title": "הַשּׁוּעָל הַמָּהִיר (״הַחוּם״) לֹא יָכוֹל לִקְפֹּץ 9.94 מֶטְרִים, נָכוֹן? ברר, 1.5°C- בַּחוּץ!" }
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "東京"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
            assert_eq!(response["hits"].as_array().unwrap().len(), 1);
        })
        .await;
}

#[cfg(feature = "default")]
#[actix_rt::test]
async fn test_thai_language() {
    let server = Server::new().await;
    let index = server.index("test");

    // We don't need documents, the issue is on the query side only.
    let documents = json!([
        { "id": 0, "title": "สบู่สมุนไพรดอกดาวเรือง 100 กรัม จำนวน 6 ก้อน" },
        { "id": 1, "title": "สบู่สมุนไพรชาเขียว 100 กรัม จำนวน 6 ก้อน" },
        { "id": 2, "title": "สบู่สมุนไพรฝางแดงผสมว่านหางจรเข้ 100 กรัม จำนวน 6 ก้อน" }
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (task, _status_code) = index.update_settings(json!({"rankingRules": ["exactness"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "สบู"}), |response, code| {
            assert_eq!(code, 200, "{}", response);
        })
        .await;
}

#[actix_rt::test]
async fn search_multiple_params() {
    let index = shared_index_with_documents().await;
    index
        .search(
            json!({
                "q": "glass",
                "attributesToCrop": ["title:2"],
                "attributesToHighlight": ["title"],
                "limit": 1,
                "offset": 0,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            },
        )
        .await;

    let index = shared_index_with_nested_documents().await;
    index
        .search(
            json!({
                "q": "pésti",
                "attributesToCrop": ["catto:2"],
                "attributesToHighlight": ["catto"],
                "limit": 2,
                "offset": 0,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 2);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_filter_string_notation() {
    let server = Server::new().await;
    let index = server.index("test");

    let (_, code) = index.update_settings(json!({"filterableAttributes": ["title"]})).await;
    meili_snap::snapshot!(code, @"202 Accepted");

    let documents = DOCUMENTS.clone();
    let (task, code) = index.add_documents(documents, None).await;
    meili_snap::snapshot!(code, @"202 Accepted");
    let res = index.wait_task(task.uid()).await;
    meili_snap::snapshot!(res["status"], @r###""succeeded""###);

    index
        .search(
            json!({
                "filter": "title = Gläss"
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
            },
        )
        .await;

    let index = server.index("nested");

    let (_, code) =
        index.update_settings(json!({"filterableAttributes": ["cattos", "doggos.age"]})).await;
    meili_snap::snapshot!(code, @"202 Accepted");

    let documents = NESTED_DOCUMENTS.clone();
    let (task, code) = index.add_documents(documents, None).await;
    meili_snap::snapshot!(code, @"202 Accepted");
    let res = index.wait_task(task.uid()).await;
    meili_snap::snapshot!(res["status"], @r###""succeeded""###);

    index
        .search(
            json!({
                "filter": "cattos = pésti"
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1);
                assert_eq!(response["hits"][0]["id"], json!(852));
            },
        )
        .await;

    index
        .search(
            json!({
                "filter": "doggos.age > 5"
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 2);
                assert_eq!(response["hits"][0]["id"], json!(654));
                assert_eq!(response["hits"][1]["id"], json!(951));
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_filter_array_notation() {
    let index = shared_index_with_documents().await;
    let (response, code) = index
        .search_post(json!({
            "filter": ["title = Gläss"]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 1);

    let (response, code) = index
        .search_post(json!({
            "filter": [["title = Gläss", "title = \"Shazam!\"", "title = \"Escape Room\""]]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 3);
}

#[actix_rt::test]
async fn search_with_contains_filter() {
    let temp = TempDir::new().unwrap();
    let server = Server::new_with_options(Opt {
        experimental_contains_filter: true,
        ..default_settings(temp.path())
    })
    .await
    .unwrap();
    let index = server.index("movies");

    index.update_settings(json!({"filterableAttributes": ["title"]})).await;

    let documents = DOCUMENTS.clone();
    let (request, _code) = index.add_documents(documents, None).await;
    index.wait_task(request.uid()).await.succeeded();

    let (response, code) = index
        .search_post(json!({
            "filter": "title CONTAINS cap"
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn search_with_sort_on_numbers() {
    let index = shared_index_with_documents().await;
    index
        .search(
            json!({
                "sort": ["id:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 5);
            },
        )
        .await;

    let index = shared_index_with_nested_documents().await;
    index
        .search(
            json!({
                "sort": ["doggos.age:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 4);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_sort_on_strings() {
    let index = shared_index_with_documents().await;
    index
        .search(
            json!({
                "sort": ["title:desc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 5);
            },
        )
        .await;

    let index = shared_index_with_nested_documents().await;
    index
        .search(
            json!({
                "sort": ["doggos.name:asc"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 4);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_with_multiple_sort() {
    let index = shared_index_with_documents().await;
    let (response, code) = index
        .search_post(json!({
            "sort": ["id:asc", "title:desc"]
        }))
        .await;
    assert_eq!(code, 200, "{}", response);
    assert_eq!(response["hits"].as_array().unwrap().len(), 5);
}

#[actix_rt::test]
async fn search_facet_distribution() {
    let index = shared_index_with_documents().await;
    index
        .search(
            json!({
                "facets": ["title"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert!(dist.get("title").is_some());
            },
        )
        .await;

    let index = shared_index_with_nested_documents().await;

    // TODO: TAMO: fix the test
    index
        .search(
            json!({
                // "facets": ["father", "doggos.name"]
                "facets": ["father"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert_eq!(
                    dist["father"],
                    json!({ "jean": 1, "pierre": 1, "romain": 1, "jean-baptiste": 1})
                );
                /*
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
                */
            },
        )
        .await;

    index
        .search(
            json!({
                "facets": ["doggos.name"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
            },
        )
        .await;

    index
        .search(
            json!({
                "facets": ["doggos"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 3);
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
                assert_eq!(dist["doggos.age"], json!({ "2": 1, "4": 1, "5": 1, "6": 1, "8": 1}));
            },
        )
        .await;

    index
        .search(
            json!({
                "facets": ["doggos.name"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let dist = response["facetDistribution"].as_object().unwrap();
                assert_eq!(dist.len(), 1);
                assert_eq!(
                    dist["doggos.name"],
                    json!({ "bobby": 1, "buddy": 1, "gros bill": 1, "turbo": 1, "fast": 1})
                );
            },
        )
        .await;
}

#[actix_rt::test]
async fn displayed_attributes() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({ "displayedAttributes": ["title"] })).await;

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) =
        index.search_post(json!({ "attributesToRetrieve": ["title", "id"] })).await;
    assert_eq!(code, 200, "{}", response);
    assert!(response["hits"][0].get("title").is_some());
}

#[actix_rt::test]
async fn placeholder_search_is_hard_limited() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents: Vec<_> = (0..1200).map(|i| json!({ "id": i, "text": "I am unique!" })).collect();
    let (task, _status_code) = index.add_documents(documents.into(), None).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1000);
            },
        )
        .await;

    index
        .search(
            json!({
                "offset": 800,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;

    let (task, _status_code) =
        index.update_settings(json!({ "pagination": { "maxTotalHits": 10_000 } })).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1200);
            },
        )
        .await;

    index
        .search(
            json!({
                "offset": 1000,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;
}

#[actix_rt::test]
async fn search_is_hard_limited() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents: Vec<_> = (0..1200).map(|i| json!({ "id": i, "text": "I am unique!" })).collect();
    let (task, _status_code) = index.add_documents(documents.into(), None).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "q": "unique",
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1000);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": "unique",
                "offset": 800,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;

    let (task, _status_code) =
        index.update_settings(json!({ "pagination": { "maxTotalHits": 10_000 } })).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "q": "unique",
                "limit": 1500,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 1200);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": "unique",
                "offset": 1000,
                "limit": 400,
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                assert_eq!(response["hits"].as_array().unwrap().len(), 200);
            },
        )
        .await;
}

#[actix_rt::test]
async fn faceting_max_values_per_facet() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({ "filterableAttributes": ["number"] })).await;

    let documents: Vec<_> = (0..10_000).map(|id| json!({ "id": id, "number": id * 10 })).collect();
    let (task, _status_code) = index.add_documents(json!(documents), None).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "facets": ["number"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let numbers = response["facetDistribution"]["number"].as_object().unwrap();
                assert_eq!(numbers.len(), 100);
            },
        )
        .await;

    let (task, _status_code) =
        index.update_settings(json!({ "faceting": { "maxValuesPerFacet": 10_000 } })).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(
            json!({
                "facets": ["number"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                let numbers = &response["facetDistribution"]["number"].as_object().unwrap();
                assert_eq!(numbers.len(), 10_000);
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_score_details() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = DOCUMENTS.clone();

    let res = index.add_documents(json!(documents), None).await;
    index.wait_task(res.0.uid()).await.succeeded();

    index
        .search(
            json!({
                "q": "train dragon",
                "showRankingScoreDetails": true,
            }),
            |response, code| {
                meili_snap::snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "How to Train Your Dragon: The Hidden World",
                    "id": "166428",
                    "color": [
                      "green",
                      "red"
                    ],
                    "_rankingScoreDetails": {
                      "words": {
                        "order": 0,
                        "matchingWords": 2,
                        "maxMatchingWords": 2,
                        "score": 1.0
                      },
                      "typo": {
                        "order": 1,
                        "typoCount": 0,
                        "maxTypoCount": 2,
                        "score": 1.0
                      },
                      "proximity": {
                        "order": 2,
                        "score": 0.75
                      },
                      "attribute": {
                        "order": 3,
                        "attributeRankingOrderScore": 1.0,
                        "queryWordDistanceScore": 0.8095238095238095,
                        "score": 0.8095238095238095
                      },
                      "exactness": {
                        "order": 4,
                        "matchType": "noExactMatch",
                        "matchingWords": 2,
                        "maxMatchingWords": 2,
                        "score": 0.3333333333333333
                      }
                    }
                  }
                ]
                "###);
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_score() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = SCORE_DOCUMENTS.clone();

    let res = index.add_documents(json!(documents), None).await;
    index.wait_task(res.0.uid()).await.succeeded();

    index
        .search(
            json!({
                "q": "Badman the dark knight returns 1",
                "showRankingScore": true,
            }),
            |response, code| {
                meili_snap::snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Batman the dark knight returns: Part 1",
                    "id": "A",
                    "_rankingScore": 0.9746605609456898
                  },
                  {
                    "title": "Batman the dark knight returns: Part 2",
                    "id": "B",
                    "_rankingScore": 0.8055252965383685
                  },
                  {
                    "title": "Badman",
                    "id": "E",
                    "_rankingScore": 0.16666666666666666
                  },
                  {
                    "title": "Batman Returns",
                    "id": "C",
                    "_rankingScore": 0.07702020202020202
                  },
                  {
                    "title": "Batman",
                    "id": "D",
                    "_rankingScore": 0.07702020202020202
                  }
                ]
                "###);
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_score_threshold() {
    let query = "Badman dark returns 1";
    let server = Server::new().await;
    let index = server.index("test");

    let documents = SCORE_DOCUMENTS.clone();

    let res = index.add_documents(json!(documents), None).await;
    index.wait_task(res.0.uid()).await.succeeded();

    index
        .search(
            json!({
                "q": query,
                "showRankingScore": true,
                "rankingScoreThreshold": 0.0
            }),
            |response, code| {
                meili_snap::snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["estimatedTotalHits"]), @"5");
                meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Batman the dark knight returns: Part 1",
                    "id": "A",
                    "_rankingScore": 0.93430081300813
                  },
                  {
                    "title": "Batman the dark knight returns: Part 2",
                    "id": "B",
                    "_rankingScore": 0.6685627880184332
                  },
                  {
                    "title": "Badman",
                    "id": "E",
                    "_rankingScore": 0.25
                  },
                  {
                    "title": "Batman Returns",
                    "id": "C",
                    "_rankingScore": 0.11553030303030302
                  },
                  {
                    "title": "Batman",
                    "id": "D",
                    "_rankingScore": 0.11553030303030302
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": query,
                "showRankingScore": true,
                "rankingScoreThreshold": 0.2
            }),
            |response, code| {
                meili_snap::snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["estimatedTotalHits"]), @r###"3"###);
                meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Batman the dark knight returns: Part 1",
                    "id": "A",
                    "_rankingScore": 0.93430081300813
                  },
                  {
                    "title": "Batman the dark knight returns: Part 2",
                    "id": "B",
                    "_rankingScore": 0.6685627880184332
                  },
                  {
                    "title": "Badman",
                    "id": "E",
                    "_rankingScore": 0.25
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": query,
                "showRankingScore": true,
                "rankingScoreThreshold": 0.5
            }),
            |response, code| {
                meili_snap::snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["estimatedTotalHits"]), @r###"2"###);
                meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Batman the dark knight returns: Part 1",
                    "id": "A",
                    "_rankingScore": 0.93430081300813
                  },
                  {
                    "title": "Batman the dark knight returns: Part 2",
                    "id": "B",
                    "_rankingScore": 0.6685627880184332
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": query,
                "showRankingScore": true,
                "rankingScoreThreshold": 0.8
            }),
            |response, code| {
                meili_snap::snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["estimatedTotalHits"]), @r###"1"###);
                meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Batman the dark knight returns: Part 1",
                    "id": "A",
                    "_rankingScore": 0.93430081300813
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .search(
            json!({
                "q": query,
                "showRankingScore": true,
                "rankingScoreThreshold": 1.0
            }),
            |response, code| {
                meili_snap::snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["estimatedTotalHits"]), @r###"0"###);
                // nobody is perfect
                meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @"[]");
            },
        )
        .await;
}

#[actix_rt::test]
async fn test_degraded_score_details() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = NESTED_DOCUMENTS.clone();

    index.add_documents(json!(documents), None).await;
    // We can't really use anything else than 0ms here; otherwise, the test will get flaky.
    let (res, _code) = index.update_settings(json!({ "searchCutoffMs": 0 })).await;
    index.wait_task(res.uid()).await.succeeded();

    index
        .search(
            json!({
                "q": "b",
                "attributesToRetrieve": ["doggos.name", "cattos"],
                "showRankingScoreDetails": true,
            }),
            |response, code| {
                meili_snap::snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response, { ".processingTimeMs" => "[duration]" }), @r###"
                {
                  "hits": [
                    {
                      "doggos": [
                        {
                          "name": "bobby"
                        },
                        {
                          "name": "buddy"
                        }
                      ],
                      "cattos": "pésti",
                      "_rankingScoreDetails": {
                        "skipped": {
                          "order": 0
                        }
                      }
                    },
                    {
                      "doggos": [
                        {
                          "name": "gros bill"
                        }
                      ],
                      "cattos": [
                        "simba",
                        "pestiféré"
                      ],
                      "_rankingScoreDetails": {
                        "skipped": {
                          "order": 0
                        }
                      }
                    },
                    {
                      "doggos": [
                        {
                          "name": "turbo"
                        },
                        {
                          "name": "fast"
                        }
                      ],
                      "cattos": [
                        "moumoute",
                        "gomez"
                      ],
                      "_rankingScoreDetails": {
                        "skipped": {
                          "order": 0
                        }
                      }
                    }
                  ],
                  "query": "b",
                  "processingTimeMs": "[duration]",
                  "limit": 20,
                  "offset": 0,
                  "estimatedTotalHits": 3
                }
                "###);
            },
        )
        .await;
}

#[cfg(feature = "default")]
#[actix_rt::test]
async fn camelcased_words() {
    let server = Server::new().await;
    let index = server.index("test");

    // related to https://github.com/meilisearch/meilisearch/issues/3818
    let documents = json!([
        { "id": 0, "title": "DeLonghi" },
        { "id": 1, "title": "delonghi" },
        { "id": 2, "title": "TestAB" },
        { "id": 3, "title": "TestAb" },
        { "id": 4, "title": "testab" },
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "deLonghi"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 0,
                "title": "DeLonghi"
              },
              {
                "id": 1,
                "title": "delonghi"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "dellonghi"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 0,
                "title": "DeLonghi"
              },
              {
                "id": 1,
                "title": "delonghi"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "testa"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "testab"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "TestaB"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "Testab"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "TestAb"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    // with Typos
    index
        .search(json!({"q": "dellonghi"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 0,
                "title": "DeLonghi"
              },
              {
                "id": 1,
                "title": "delonghi"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "TetsAB"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "TetsAB"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "id": 2,
                "title": "TestAB"
              },
              {
                "id": 3,
                "title": "TestAb"
              },
              {
                "id": 4,
                "title": "testab"
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn simple_search_with_strange_synonyms() {
    let server = Server::new().await;
    let index = server.index("test");

    let (task, _status_code) =
        index.update_settings(json!({ "synonyms": {"&": ["to"], "to": ["&"]} })).await;
    let r = index.wait_task(task.uid()).await;
    meili_snap::snapshot!(r["status"], @r###""succeeded""###);

    let documents = DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();

    index
        .search(json!({"q": "How to train"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "id": "166428",
                "color": [
                  "green",
                  "red"
                ]
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "How & train"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "id": "166428",
                "color": [
                  "green",
                  "red"
                ]
              }
            ]
            "###);
        })
        .await;

    index
        .search(json!({"q": "to"}), |response, code| {
            meili_snap::snapshot!(code, @"200 OK");
            meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "id": "166428",
                "color": [
                  "green",
                  "red"
                ]
              }
            ]
            "###);
        })
        .await;
}

#[actix_rt::test]
async fn change_attributes_settings() {
    let server = Server::new().await;
    let index = server.index("test");

    index.update_settings(json!({ "searchableAttributes": ["father", "mother"] })).await;

    let documents = NESTED_DOCUMENTS.clone();
    let (task, _status_code) = index.add_documents(json!(documents), None).await;
    index.wait_task(task.uid()).await.succeeded();

    let (task,_status_code) =
        index.update_settings(json!({ "searchableAttributes": ["father", "mother", "doggos"], "filterableAttributes": ["doggos"] })).await;
    index.wait_task(task.uid()).await.succeeded();

    // search
    index
        .search(
            json!({
                "q": "bobby",
                "attributesToRetrieve": ["id", "doggos"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 852,
                    "doggos": [
                      {
                        "name": "bobby",
                        "age": 2
                      },
                      {
                        "name": "buddy",
                        "age": 4
                      }
                    ]
                  }
                ]
                "###);
            },
        )
        .await;

    // filter
    index
        .search(
            json!({
                "q": "",
                "filter": "doggos.age < 5",
                "attributesToRetrieve": ["id", "doggos"]
            }),
            |response, code| {
                assert_eq!(code, 200, "{}", response);
                meili_snap::snapshot!(meili_snap::json_string!(response["hits"]), @r###"
                [
                  {
                    "id": 852,
                    "doggos": [
                      {
                        "name": "bobby",
                        "age": 2
                      },
                      {
                        "name": "buddy",
                        "age": 4
                      }
                    ]
                  }
                ]
                "###);
            },
        )
        .await;
}
