use crate::common::Server;
use crate::json;

macro_rules! test_setting_routes {
    ($({setting: $setting:ident, update_verb: $update_verb:ident, default_value: $default_value:tt},) *) => {
        $(
            mod $setting {
                use crate::common::Server;

                #[actix_rt::test]
                async fn get_unexisting_index() {
                    let server = Server::new().await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (_response, code) = server.service.get(url).await;
                    assert_eq!(code, 404);
                }

                #[actix_rt::test]
                async fn update_unexisting_index() {
                    let server = Server::new().await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (response, code) = server.service.$update_verb(url, serde_json::Value::Null.into()).await;
                    assert_eq!(code, 202, "{}", response);
                    server.index("").wait_task(0).await;
                    let (response, code) = server.index("test").get().await;
                    assert_eq!(code, 200, "{}", response);
                }

                #[actix_rt::test]
                async fn delete_unexisting_index() {
                    let server = Server::new().await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (_, code) = server.service.delete(url).await;
                    assert_eq!(code, 202);
                    let response = server.index("").wait_task(0).await;
                    assert_eq!(response["status"], "failed");
                }

                #[actix_rt::test]
                async fn get_default() {
                    let server = Server::new().await;
                    let index = server.index("test");
                    let (response, code) = index.create(None).await;
                    assert_eq!(code, 202, "{}", response);
                    index.wait_task(0).await;
                    let url = format!("/indexes/test/settings/{}",
                        stringify!($setting)
                        .chars()
                        .map(|c| if c == '_' { '-' } else { c })
                        .collect::<String>());
                    let (response, code) = server.service.get(url).await;
                    assert_eq!(code, 200, "{}", response);
                    let expected = crate::json!($default_value);
                    assert_eq!(expected, response);
                }
            }
        )*

        #[actix_rt::test]
        async fn all_setting_tested() {
            let expected = std::collections::BTreeSet::from_iter(meilisearch::routes::indexes::settings::ALL_SETTINGS_NAMES.iter());
            let tested = std::collections::BTreeSet::from_iter([$(stringify!($setting)),*].iter());
            let diff: Vec<_> = expected.difference(&tested).collect();
            assert!(diff.is_empty(), "Not all settings were tested, please add the following settings to the `test_setting_routes!` macro: {:?}", diff);
        }
    };
}

test_setting_routes!(
    {
        setting: filterable_attributes,
        update_verb: put,
        default_value: []
    },
    {
        setting: displayed_attributes,
        update_verb: put,
        default_value: ["*"]
    },
    {
        setting: localized_attributes,
        update_verb: put,
        default_value: null
    },
    {
        setting: searchable_attributes,
        update_verb: put,
        default_value: ["*"]
    },
    {
        setting: distinct_attribute,
        update_verb: put,
        default_value: null
    },
    {
        setting: stop_words,
        update_verb: put,
        default_value: []
    },
    {
        setting: separator_tokens,
        update_verb: put,
        default_value: []
    },
    {
        setting: non_separator_tokens,
        update_verb: put,
        default_value: []
    },
    {
        setting: dictionary,
        update_verb: put,
        default_value: []
    },
    {
        setting: ranking_rules,
        update_verb: put,
        default_value: ["words", "typo", "proximity", "attribute", "sort", "exactness"]
    },
    {
        setting: synonyms,
        update_verb: put,
        default_value: {}
    },
    {
        setting: pagination,
        update_verb: patch,
        default_value: {"maxTotalHits": 1000}
    },
    {
        setting: faceting,
        update_verb: patch,
        default_value: {"maxValuesPerFacet": 100, "sortFacetValuesBy": {"*": "alpha"}}
    },
    {
        setting: search_cutoff_ms,
        update_verb: put,
        default_value: null
    },
    {
        setting: embedders,
        update_verb: patch,
        default_value: {}
    },
    {
        setting: facet_search,
        update_verb: put,
        default_value: true
    },
    {
        setting: prefix_search,
        update_verb: put,
        default_value: "indexingTime"
    },
    {
        setting: proximity_precision,
        update_verb: put,
        default_value: "byWord"
    },
    {
        setting: sortable_attributes,
        update_verb: put,
        default_value: []
    },
    {
        setting: typo_tolerance,
        update_verb: patch,
        default_value: {"enabled": true, "minWordSizeForTypos": {"oneTypo": 5, "twoTypos": 9}, "disableOnWords": [], "disableOnAttributes": []}
    },
);

#[actix_rt::test]
async fn get_settings_unexisting_index() {
    let server = Server::new().await;
    let (response, code) = server.index("test").settings().await;
    assert_eq!(code, 404, "{}", response)
}

#[actix_rt::test]
async fn get_settings() {
    let server = Server::new().await;
    let index = server.index("test");
    let (response, _code) = index.create(None).await;
    index.wait_task(response.uid()).await.succeeded();
    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    let settings = response.as_object().unwrap();
    assert_eq!(settings.keys().len(), 20);
    assert_eq!(settings["displayedAttributes"], json!(["*"]));
    assert_eq!(settings["searchableAttributes"], json!(["*"]));
    assert_eq!(settings["filterableAttributes"], json!([]));
    assert_eq!(settings["sortableAttributes"], json!([]));
    assert_eq!(settings["distinctAttribute"], json!(null));
    assert_eq!(
        settings["rankingRules"],
        json!(["words", "typo", "proximity", "attribute", "sort", "exactness"])
    );
    assert_eq!(settings["stopWords"], json!([]));
    assert_eq!(settings["nonSeparatorTokens"], json!([]));
    assert_eq!(settings["separatorTokens"], json!([]));
    assert_eq!(settings["dictionary"], json!([]));
    assert_eq!(
        settings["faceting"],
        json!({
            "maxValuesPerFacet": 100,
            "sortFacetValuesBy": {
                "*": "alpha"
            }
        })
    );
    assert_eq!(
        settings["pagination"],
        json!({
            "maxTotalHits": 1000,
        })
    );
    assert_eq!(settings["proximityPrecision"], json!("byWord"));
    assert_eq!(settings["searchCutoffMs"], json!(null));
    assert_eq!(settings["prefixSearch"], json!("indexingTime"));
    assert_eq!(settings["facetSearch"], json!(true));
    assert_eq!(settings["embedders"], json!({}));
}

#[actix_rt::test]
async fn secrets_are_hidden_in_settings() {
    let server = Server::new().await;

    let index = server.index("test");
    let (response, _code) = index.create(None).await;
    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index
        .update_settings(json!({
            "embedders": {
                "default": {
                    "source": "rest",
                    "url": "https://localhost:7777",
                    "apiKey": "My super secret value you will never guess",
                    "dimensions": 4,
                    "request": "{{text}}",
                    "response": "{{embedding}}"
                }
            }
        }))
        .await;
    meili_snap::snapshot!(code, @"202 Accepted");

    meili_snap::snapshot!(meili_snap::json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
    @r###"
    {
      "taskUid": 1,
      "indexUid": "test",
      "status": "enqueued",
      "type": "settingsUpdate",
      "enqueuedAt": "[date]"
    }
    "###);

    let settings_update_uid = response.uid();

    index.wait_task(settings_update_uid).await;

    let (response, code) = index.settings().await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r#"
    {
      "displayedAttributes": [
        "*"
      ],
      "searchableAttributes": [
        "*"
      ],
      "filterableAttributes": [],
      "sortableAttributes": [],
      "rankingRules": [
        "words",
        "typo",
        "proximity",
        "attribute",
        "sort",
        "exactness"
      ],
      "stopWords": [],
      "nonSeparatorTokens": [],
      "separatorTokens": [],
      "dictionary": [],
      "synonyms": {},
      "distinctAttribute": null,
      "proximityPrecision": "byWord",
      "typoTolerance": {
        "enabled": true,
        "minWordSizeForTypos": {
          "oneTypo": 5,
          "twoTypos": 9
        },
        "disableOnWords": [],
        "disableOnAttributes": []
      },
      "faceting": {
        "maxValuesPerFacet": 100,
        "sortFacetValuesBy": {
          "*": "alpha"
        }
      },
      "pagination": {
        "maxTotalHits": 1000
      },
      "embedders": {
        "default": {
          "source": "rest",
          "apiKey": "My suXXXXXX...",
          "dimensions": 4,
          "documentTemplate": "{% for field in fields %}{% if field.is_searchable and field.value != nil %}{{ field.name }}: {{ field.value }}\n{% endif %}{% endfor %}",
          "documentTemplateMaxBytes": 400,
          "url": "https://localhost:7777",
          "request": "{{text}}",
          "response": "{{embedding}}",
          "headers": {}
        }
      },
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "#);

    let (response, code) = server.get_task(settings_update_uid).await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response["details"]), @r#"
    {
      "embedders": {
        "default": {
          "source": "rest",
          "apiKey": "My suXXXXXX...",
          "dimensions": 4,
          "url": "https://localhost:7777",
          "request": "{{text}}",
          "response": "{{embedding}}"
        }
      }
    }
    "#);
}

#[actix_rt::test]
async fn error_update_settings_unknown_field() {
    let server = Server::new().await;
    let index = server.index("test");
    let (_response, code) = index.update_settings(json!({"foo": 12})).await;
    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn test_partial_update() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, _code) = index.update_settings(json!({"displayedAttributes": ["foo"]})).await;
    index.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"], json!(["foo"]));
    assert_eq!(response["searchableAttributes"], json!(["*"]));

    let (task, _) = index.update_settings(json!({"searchableAttributes": ["bar"]})).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"], json!(["foo"]));
    assert_eq!(response["searchableAttributes"], json!(["bar"]));
}

#[actix_rt::test]
async fn error_delete_settings_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, code) = index.delete_settings().await;
    assert_eq!(code, 202);

    let response = index.wait_task(task.uid()).await;

    assert_eq!(response["status"], "failed");
}

#[actix_rt::test]
async fn reset_all_settings() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "id": 1,
            "name": "curqui",
            "age": 99
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    assert_eq!(response["taskUid"], 0);
    index.wait_task(response.uid()).await.succeeded();

    let (update_task,_status_code) = index
        .update_settings(json!({"displayedAttributes": ["name", "age"], "searchableAttributes": ["name"], "stopWords": ["the"], "filterableAttributes": ["age"], "synonyms": {"puppy": ["dog", "doggo", "potat"] }}))
        .await;
    index.wait_task(update_task.uid()).await.succeeded();
    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"], json!(["name", "age"]));
    assert_eq!(response["searchableAttributes"], json!(["name"]));
    assert_eq!(response["stopWords"], json!(["the"]));
    assert_eq!(response["synonyms"], json!({"puppy": ["dog", "doggo", "potat"] }));
    assert_eq!(response["filterableAttributes"], json!(["age"]));

    let (delete_task, _status_code) = index.delete_settings().await;
    index.wait_task(delete_task.uid()).await.succeeded();

    let (response, code) = index.settings().await;
    assert_eq!(code, 200);
    assert_eq!(response["displayedAttributes"], json!(["*"]));
    assert_eq!(response["searchableAttributes"], json!(["*"]));
    assert_eq!(response["stopWords"], json!([]));
    assert_eq!(response["filterableAttributes"], json!([]));
    assert_eq!(response["synonyms"], json!({}));

    let (response, code) = index.get_document(1, None).await;
    assert_eq!(code, 200);
    assert!(response.as_object().unwrap().get("age").is_some());
}

#[actix_rt::test]
async fn update_setting_unexisting_index() {
    let server = Server::new().await;
    let index = server.index("test");
    let (task, code) = index.update_settings(json!({})).await;
    assert_eq!(code, 202);
    let response = index.wait_task(task.uid()).await;
    assert_eq!(response["status"], "succeeded");
    let (_response, code) = index.get().await;
    assert_eq!(code, 200);
    let (task, _status_code) = index.delete_settings().await;
    let response = index.wait_task(task.uid()).await;
    assert_eq!(response["status"], "succeeded");
}

#[actix_rt::test]
async fn error_update_setting_unexisting_index_invalid_uid() {
    let server = Server::new().await;
    let index = server.index("test##!  ");
    let (response, code) = index.update_settings(json!({})).await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "`test##!  ` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn error_set_invalid_ranking_rules() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(None).await;

    let (response, code) = index.update_settings(json!({ "rankingRules": [ "manyTheFish"]})).await;
    meili_snap::snapshot!(code, @"400 Bad Request");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
    {
      "message": "Invalid value at `.rankingRules[0]`: `manyTheFish` ranking rule is invalid. Valid ranking rules are words, typo, sort, proximity, attribute, exactness and custom ranking rules.",
      "code": "invalid_settings_ranking_rules",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_settings_ranking_rules"
    }
    "###);
}

#[actix_rt::test]
async fn set_and_reset_distinct_attribute_with_dedicated_route() {
    let server = Server::new().await;
    let index = server.index("test");

    let (task, _code) = index.update_distinct_attribute(json!("test")).await;
    index.wait_task(task.uid()).await.succeeded();

    let (response, _) = index.get_distinct_attribute().await;

    assert_eq!(response, "test");

    let (task, _status_code) = index.update_distinct_attribute(json!(null)).await;

    index.wait_task(task.uid()).await.succeeded();

    let (response, _) = index.get_distinct_attribute().await;

    assert_eq!(response, json!(null));
}
