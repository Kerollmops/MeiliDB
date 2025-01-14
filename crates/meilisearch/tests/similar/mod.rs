mod errors;

use meili_snap::{json_string, snapshot};
use once_cell::sync::Lazy;

use crate::common::{Server, Value};
use crate::json;

static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Shazam!",
            "release_year": 2019,
            "id": "287947",
            // Three semantic properties:
            // 1. magic, anything that reminds you of magic
            // 2. authority, anything that inspires command
            // 3. horror, anything that inspires fear or dread
            "_vectors": { "manual": [0.8, 0.4, -0.5]},
        },
        {
            "title": "Captain Marvel",
            "release_year": 2019,
            "id": "299537",
            "_vectors": { "manual": [0.6, 0.8, -0.2] },
        },
        {
            "title": "Escape Room",
            "release_year": 2019,
            "id": "522681",
            "_vectors": { "manual": [0.1, 0.6, 0.8] },
        },
        {
            "title": "How to Train Your Dragon: The Hidden World",
            "release_year": 2019,
            "id": "166428",
            "_vectors": { "manual": [0.7, 0.7, -0.4] },
        },
        {
            "title": "All Quiet on the Western Front",
            "release_year": 1930,
            "id": "143",
            "_vectors": { "manual": [-0.5, 0.3, 0.85] },
        }
    ])
});

#[actix_rt::test]
async fn basic() {
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
        .similar(
            json!({"id": 143, "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "Escape Room",
                "release_year": 2019,
                "id": "522681",
                "_vectors": {
                  "manual": {
                    "embeddings": [
                      [
                        0.10000000149011612,
                        0.6000000238418579,
                        0.800000011920929
                      ]
                    ],
                    "regenerate": false
                  }
                }
              },
              {
                "title": "Captain Marvel",
                "release_year": 2019,
                "id": "299537",
                "_vectors": {
                  "manual": {
                    "embeddings": [
                      [
                        0.6000000238418579,
                        0.800000011920929,
                        -0.20000000298023224
                      ]
                    ],
                    "regenerate": false
                  }
                }
              },
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "release_year": 2019,
                "id": "166428",
                "_vectors": {
                  "manual": {
                    "embeddings": [
                      [
                        0.699999988079071,
                        0.699999988079071,
                        -0.4000000059604645
                      ]
                    ],
                    "regenerate": false
                  }
                }
              },
              {
                "title": "Shazam!",
                "release_year": 2019,
                "id": "287947",
                "_vectors": {
                  "manual": {
                    "embeddings": [
                      [
                        0.800000011920929,
                        0.4000000059604645,
                        -0.5
                      ]
                    ],
                    "regenerate": false
                  }
                }
              }
            ]
            "###);
            },
        )
        .await;

    index
        .similar(
            json!({"id": "299537", "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "How to Train Your Dragon: The Hidden World",
                "release_year": 2019,
                "id": "166428",
                "_vectors": {
                  "manual": {
                    "embeddings": [
                      [
                        0.699999988079071,
                        0.699999988079071,
                        -0.4000000059604645
                      ]
                    ],
                    "regenerate": false
                  }
                }
              },
              {
                "title": "Shazam!",
                "release_year": 2019,
                "id": "287947",
                "_vectors": {
                  "manual": {
                    "embeddings": [
                      [
                        0.800000011920929,
                        0.4000000059604645,
                        -0.5
                      ]
                    ],
                    "regenerate": false
                  }
                }
              },
              {
                "title": "Escape Room",
                "release_year": 2019,
                "id": "522681",
                "_vectors": {
                  "manual": {
                    "embeddings": [
                      [
                        0.10000000149011612,
                        0.6000000238418579,
                        0.800000011920929
                      ]
                    ],
                    "regenerate": false
                  }
                }
              },
              {
                "title": "All Quiet on the Western Front",
                "release_year": 1930,
                "id": "143",
                "_vectors": {
                  "manual": {
                    "embeddings": [
                      [
                        -0.5,
                        0.30000001192092896,
                        0.8500000238418579
                      ]
                    ],
                    "regenerate": false
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
async fn ranking_score_threshold() {
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
        .similar(
            json!({"id": 143, "showRankingScore": true, "rankingScoreThreshold": 0, "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["estimatedTotalHits"]), @"4");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Escape Room",
                    "release_year": 2019,
                    "id": "522681",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.10000000149011612,
                            0.6000000238418579,
                            0.800000011920929
                          ]
                        ],
                        "regenerate": false
                      }
                    },
                    "_rankingScore": 0.890957772731781
                  },
                  {
                    "title": "Captain Marvel",
                    "release_year": 2019,
                    "id": "299537",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.6000000238418579,
                            0.800000011920929,
                            -0.20000000298023224
                          ]
                        ],
                        "regenerate": false
                      }
                    },
                    "_rankingScore": 0.39060014486312866
                  },
                  {
                    "title": "How to Train Your Dragon: The Hidden World",
                    "release_year": 2019,
                    "id": "166428",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.699999988079071,
                            0.699999988079071,
                            -0.4000000059604645
                          ]
                        ],
                        "regenerate": false
                      }
                    },
                    "_rankingScore": 0.2819308042526245
                  },
                  {
                    "title": "Shazam!",
                    "release_year": 2019,
                    "id": "287947",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.800000011920929,
                            0.4000000059604645,
                            -0.5
                          ]
                        ],
                        "regenerate": false
                      }
                    },
                    "_rankingScore": 0.1662663221359253
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .similar(
            json!({"id": 143, "showRankingScore": true, "rankingScoreThreshold": 0.2, "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["estimatedTotalHits"]), @"3");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Escape Room",
                    "release_year": 2019,
                    "id": "522681",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.10000000149011612,
                            0.6000000238418579,
                            0.800000011920929
                          ]
                        ],
                        "regenerate": false
                      }
                    },
                    "_rankingScore": 0.890957772731781
                  },
                  {
                    "title": "Captain Marvel",
                    "release_year": 2019,
                    "id": "299537",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.6000000238418579,
                            0.800000011920929,
                            -0.20000000298023224
                          ]
                        ],
                        "regenerate": false
                      }
                    },
                    "_rankingScore": 0.39060014486312866
                  },
                  {
                    "title": "How to Train Your Dragon: The Hidden World",
                    "release_year": 2019,
                    "id": "166428",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.699999988079071,
                            0.699999988079071,
                            -0.4000000059604645
                          ]
                        ],
                        "regenerate": false
                      }
                    },
                    "_rankingScore": 0.2819308042526245
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .similar(
            json!({"id": 143, "showRankingScore": true, "rankingScoreThreshold": 0.3, "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["estimatedTotalHits"]), @"2");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Escape Room",
                    "release_year": 2019,
                    "id": "522681",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.10000000149011612,
                            0.6000000238418579,
                            0.800000011920929
                          ]
                        ],
                        "regenerate": false
                      }
                    },
                    "_rankingScore": 0.890957772731781
                  },
                  {
                    "title": "Captain Marvel",
                    "release_year": 2019,
                    "id": "299537",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.6000000238418579,
                            0.800000011920929,
                            -0.20000000298023224
                          ]
                        ],
                        "regenerate": false
                      }
                    },
                    "_rankingScore": 0.39060014486312866
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .similar(
            json!({"id": 143, "showRankingScore": true, "rankingScoreThreshold": 0.6, "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                meili_snap::snapshot!(meili_snap::json_string!(response["estimatedTotalHits"]), @"1");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Escape Room",
                    "release_year": 2019,
                    "id": "522681",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.10000000149011612,
                            0.6000000238418579,
                            0.800000011920929
                          ]
                        ],
                        "regenerate": false
                      }
                    },
                    "_rankingScore": 0.890957772731781
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .similar(
            json!({"id": 143, "showRankingScore": true, "rankingScoreThreshold": 0.9, "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @"[]");
            },
        )
        .await;
}

#[actix_rt::test]
async fn filter() {
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
        "filterableAttributes": ["title", "release_year"]}))
        .await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await;

    let documents = DOCUMENTS.clone();
    let (value, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    index.wait_task(value.uid()).await.succeeded();

    index
        .similar(
            json!({"id": 522681, "filter": "release_year = 2019", "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Captain Marvel",
                    "release_year": 2019,
                    "id": "299537",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.6000000238418579,
                            0.800000011920929,
                            -0.20000000298023224
                          ]
                        ],
                        "regenerate": false
                      }
                    }
                  },
                  {
                    "title": "How to Train Your Dragon: The Hidden World",
                    "release_year": 2019,
                    "id": "166428",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.699999988079071,
                            0.699999988079071,
                            -0.4000000059604645
                          ]
                        ],
                        "regenerate": false
                      }
                    }
                  },
                  {
                    "title": "Shazam!",
                    "release_year": 2019,
                    "id": "287947",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.800000011920929,
                            0.4000000059604645,
                            -0.5
                          ]
                        ],
                        "regenerate": false
                      }
                    }
                  }
                ]
                "###);
            },
        )
        .await;

    index
        .similar(
            json!({"id": 522681, "filter": "release_year < 2000", "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "All Quiet on the Western Front",
                    "release_year": 1930,
                    "id": "143",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            -0.5,
                            0.30000001192092896,
                            0.8500000238418579
                          ]
                        ],
                        "regenerate": false
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
async fn limit_and_offset() {
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
        .similar(
            json!({"id": 143, "limit": 1, "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "title": "Escape Room",
                "release_year": 2019,
                "id": "522681",
                "_vectors": {
                  "manual": {
                    "embeddings": [
                      [
                        0.10000000149011612,
                        0.6000000238418579,
                        0.800000011920929
                      ]
                    ],
                    "regenerate": false
                  }
                }
              }
            ]
            "###);
            },
        )
        .await;

    index
        .similar(
            json!({"id": 143, "limit": 1, "offset": 1, "retrieveVectors": true, "embedder": "manual"}),
            |response, code| {
                snapshot!(code, @"200 OK");
                snapshot!(json_string!(response["hits"]), @r###"
                [
                  {
                    "title": "Captain Marvel",
                    "release_year": 2019,
                    "id": "299537",
                    "_vectors": {
                      "manual": {
                        "embeddings": [
                          [
                            0.6000000238418579,
                            0.800000011920929,
                            -0.20000000298023224
                          ]
                        ],
                        "regenerate": false
                      }
                    }
                  }
                ]
                "###);
            },
        )
        .await;
}
