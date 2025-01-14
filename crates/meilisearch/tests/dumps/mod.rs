mod data;

use meili_snap::{json_string, snapshot};
use meilisearch::Opt;

use self::data::GetDump;
use crate::common::{default_settings, GetAllDocumentsOptions, Server};
use crate::json;

// all the following test are ignored on windows. See #2364
#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v1_movie_raw() {
    let temp = tempfile::tempdir().unwrap();
    let path = GetDump::MoviesRawV1.path();
    let options = Opt { import_dump: Some(path), ..default_settings(temp.path()) };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
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
        "typo",
        "words",
        "proximity",
        "attribute",
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks,
        json!({ "results": [{"uid": 0, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": { "receivedDocuments": 0, "indexedDocuments": 31968 }, "error": null, "duration": "PT9.317060500S", "enqueuedAt": "2021-09-08T09:08:45.153219Z", "startedAt": "2021-09-08T09:08:45.3961665Z", "finishedAt": "2021-09-08T09:08:54.713227Z" }], "total": 1,  "limit": 20, "from": 0, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({"id": 100, "title": "Lock, Stock and Two Smoking Barrels", "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "genres": ["Comedy", "Crime"], "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000})
    );

    let (document, code) = index.get_document(500, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({"id": 500, "title": "Reservoir Dogs", "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "genres": ["Crime", "Thriller"], "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({"id": 10006, "title": "Wild Seven", "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "genres": ["Action", "Crime", "Drama"], "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=indexUID&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v1_movie_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let path = GetDump::MoviesWithSettingsV1.path();

    let options = Opt { import_dump: Some(path), ..default_settings(temp.path()) };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(stats),
        @r###"
    {
      "numberOfDocuments": 53,
      "isIndexing": false,
      "fieldDistribution": {
        "genres": 53,
        "id": 53,
        "overview": 53,
        "poster": 53,
        "release_date": 53,
        "title": 53
      }
    }
    "###
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
    {
      "displayedAttributes": [
        "genres",
        "id",
        "overview",
        "poster",
        "release_date",
        "title"
      ],
      "searchableAttributes": [
        "title",
        "overview"
      ],
      "filterableAttributes": [
        "genres"
      ],
      "sortableAttributes": [
        "genres"
      ],
      "rankingRules": [
        "typo",
        "words",
        "proximity",
        "attribute",
        "exactness"
      ],
      "stopWords": [
        "of",
        "the"
      ],
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks,
        json!({ "results": [{ "uid": 1, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "settingsUpdate", "canceledBy": null, "details": { "displayedAttributes": ["genres", "id", "overview", "poster", "release_date", "title"], "searchableAttributes": ["title", "overview"], "filterableAttributes": ["genres"], "sortableAttributes": ["genres"], "stopWords": ["of", "the"] }, "error": null, "duration": "PT7.288826907S", "enqueuedAt": "2021-09-08T09:34:40.882977Z", "startedAt": "2021-09-08T09:34:40.883073093Z", "finishedAt": "2021-09-08T09:34:48.1719Z"}, { "uid": 0, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": { "receivedDocuments": 0, "indexedDocuments": 31968 }, "error": null, "duration": "PT9.090735774S", "enqueuedAt": "2021-09-08T09:34:16.036101Z", "startedAt": "2021-09-08T09:34:16.261191226Z", "finishedAt": "2021-09-08T09:34:25.351927Z" }], "total": 2, "limit": 20, "from": 1, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 100, "title": "Lock, Stock and Two Smoking Barrels", "genres": ["Comedy", "Crime"], "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000 })
    );

    let (document, code) = index.get_document(500, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 500, "title": "Reservoir Dogs", "genres": ["Crime", "Thriller"], "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 10006, "title": "Wild Seven", "genres": ["Action", "Crime", "Drama"], "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=indexUID&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn import_dump_v1_rubygems_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let path = GetDump::RubyGemsWithSettingsV1.path();

    let options = Opt { import_dump: Some(path), ..default_settings(temp.path()) };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("rubygems"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("rubygems");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"description": 53, "id": 53, "name": 53, "summary": 53, "total_downloads": 53, "version": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
    {
      "displayedAttributes": [
        "description",
        "id",
        "name",
        "summary",
        "total_downloads",
        "version"
      ],
      "searchableAttributes": [
        "name",
        "summary"
      ],
      "filterableAttributes": [
        "version"
      ],
      "sortableAttributes": [
        "version"
      ],
      "rankingRules": [
        "typo",
        "words",
        "fame:desc",
        "proximity",
        "attribute",
        "exactness",
        "total_downloads:desc"
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks["results"][0],
        json!({"uid": 92, "batchUid": null, "indexUid": "rubygems", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": {"receivedDocuments": 0, "indexedDocuments": 1042}, "error": null, "duration": "PT1.487793839S", "enqueuedAt": "2021-09-08T09:27:01.465296Z", "startedAt": "2021-09-08T09:28:44.882177161Z", "finishedAt": "2021-09-08T09:28:46.369971Z"})
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(188040, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "meilisearch", "summary": "An easy-to-use ruby client for Meilisearch API", "description": "An easy-to-use ruby client for Meilisearch API. See https://github.com/meilisearch/MeiliSearch", "id": "188040", "version": "0.15.2", "total_downloads": "7465"})
    );

    let (document, code) = index.get_document(191940, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "doggo", "summary": "RSpec 3 formatter - documentation, with progress indication", "description": "Similar to \"rspec -f d\", but also indicates progress by showing the current test number and total test count on each line.", "id": "191940", "version": "1.1.0", "total_downloads": "9394"})
    );

    let (document, code) = index.get_document(159227, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "vortex-of-agony", "summary": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "description": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "id": "159227", "version": "0.1.0", "total_downloads": "1007"})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=rubygems&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
async fn import_dump_v2_movie_raw() {
    let temp = tempfile::tempdir().unwrap();

    let options =
        Opt { import_dump: Some(GetDump::MoviesRawV2.path()), ..default_settings(temp.path()) };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks,
        json!({ "results": [{"uid": 0, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "error": null, "duration": "PT41.751156S", "enqueuedAt": "2021-09-08T08:30:30.550282Z", "startedAt": "2021-09-08T08:30:30.553012Z", "finishedAt": "2021-09-08T08:31:12.304168Z" }], "total": 1, "limit": 20, "from": 0, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({"id": 100, "title": "Lock, Stock and Two Smoking Barrels", "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "genres": ["Comedy", "Crime"], "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000})
    );

    let (document, code) = index.get_document(500, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({"id": 500, "title": "Reservoir Dogs", "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "genres": ["Crime", "Thriller"], "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({"id": 10006, "title": "Wild Seven", "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "genres": ["Action", "Crime", "Drama"], "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=indexUID&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
async fn import_dump_v2_movie_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::MoviesWithSettingsV2.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
    {
      "displayedAttributes": [
        "title",
        "genres",
        "overview",
        "poster",
        "release_date"
      ],
      "searchableAttributes": [
        "title",
        "overview"
      ],
      "filterableAttributes": [
        "genres"
      ],
      "sortableAttributes": [],
      "rankingRules": [
        "words",
        "typo",
        "proximity",
        "attribute",
        "exactness"
      ],
      "stopWords": [
        "of",
        "the"
      ],
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks,
        json!({ "results": [{ "uid": 1, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "settingsUpdate", "canceledBy": null, "details": { "displayedAttributes": ["title", "genres", "overview", "poster", "release_date"], "searchableAttributes": ["title", "overview"], "filterableAttributes": ["genres"], "stopWords": ["of", "the"] }, "error": null, "duration": "PT37.488777S", "enqueuedAt": "2021-09-08T08:24:02.323444Z", "startedAt": "2021-09-08T08:24:02.324145Z", "finishedAt": "2021-09-08T08:24:39.812922Z" }, { "uid": 0, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "error": null, "duration": "PT39.941318S", "enqueuedAt": "2021-09-08T08:21:14.742672Z", "startedAt": "2021-09-08T08:21:14.750166Z", "finishedAt": "2021-09-08T08:21:54.691484Z" }], "total": 2, "limit": 20, "from": 1, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 100, "title": "Lock, Stock and Two Smoking Barrels", "genres": ["Comedy", "Crime"], "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000 })
    );

    let (document, code) = index.get_document(500, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 500, "title": "Reservoir Dogs", "genres": ["Crime", "Thriller"], "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 10006, "title": "Wild Seven", "genres": ["Action", "Crime", "Drama"], "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=indexUID&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
async fn import_dump_v2_rubygems_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::RubyGemsWithSettingsV2.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("rubygems"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("rubygems");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"description": 53, "id": 53, "name": 53, "summary": 53, "total_downloads": 53, "version": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
    {
      "displayedAttributes": [
        "name",
        "summary",
        "description",
        "version",
        "total_downloads"
      ],
      "searchableAttributes": [
        "name",
        "summary"
      ],
      "filterableAttributes": [
        "version"
      ],
      "sortableAttributes": [],
      "rankingRules": [
        "typo",
        "words",
        "fame:desc",
        "proximity",
        "attribute",
        "exactness",
        "total_downloads:desc"
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks["results"][0],
        json!({"uid": 92, "batchUid": null, "indexUid": "rubygems", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": {"receivedDocuments": 0, "indexedDocuments": 1042}, "error": null, "duration": "PT14.034672S", "enqueuedAt": "2021-09-08T08:40:31.390775Z", "startedAt": "2021-09-08T08:51:39.060642Z", "finishedAt": "2021-09-08T08:51:53.095314Z"})
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(188040, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "meilisearch", "summary": "An easy-to-use ruby client for Meilisearch API", "description": "An easy-to-use ruby client for Meilisearch API. See https://github.com/meilisearch/MeiliSearch", "id": "188040", "version": "0.15.2", "total_downloads": "7465"})
    );

    let (document, code) = index.get_document(191940, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "doggo", "summary": "RSpec 3 formatter - documentation, with progress indication", "description": "Similar to \"rspec -f d\", but also indicates progress by showing the current test number and total test count on each line.", "id": "191940", "version": "1.1.0", "total_downloads": "9394"})
    );

    let (document, code) = index.get_document(159227, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "vortex-of-agony", "summary": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "description": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "id": "159227", "version": "0.1.0", "total_downloads": "1007"})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=rubygems&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
async fn import_dump_v3_movie_raw() {
    let temp = tempfile::tempdir().unwrap();

    let options =
        Opt { import_dump: Some(GetDump::MoviesRawV3.path()), ..default_settings(temp.path()) };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks,
        json!({ "results": [{"uid": 0, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "error": null, "duration": "PT41.751156S", "enqueuedAt": "2021-09-08T08:30:30.550282Z", "startedAt": "2021-09-08T08:30:30.553012Z", "finishedAt": "2021-09-08T08:31:12.304168Z" }], "total": 1, "limit": 20, "from": 0, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({"id": 100, "title": "Lock, Stock and Two Smoking Barrels", "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "genres": ["Comedy", "Crime"], "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000})
    );

    let (document, code) = index.get_document(500, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({"id": 500, "title": "Reservoir Dogs", "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "genres": ["Crime", "Thriller"], "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({"id": 10006, "title": "Wild Seven", "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "genres": ["Action", "Crime", "Drama"], "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=indexUID&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
async fn import_dump_v3_movie_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::MoviesWithSettingsV3.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
    {
      "displayedAttributes": [
        "title",
        "genres",
        "overview",
        "poster",
        "release_date"
      ],
      "searchableAttributes": [
        "title",
        "overview"
      ],
      "filterableAttributes": [
        "genres"
      ],
      "sortableAttributes": [],
      "rankingRules": [
        "words",
        "typo",
        "proximity",
        "attribute",
        "exactness"
      ],
      "stopWords": [
        "of",
        "the"
      ],
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks,
        json!({ "results": [{ "uid": 1, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "settingsUpdate", "canceledBy": null, "details": { "displayedAttributes": ["title", "genres", "overview", "poster", "release_date"], "searchableAttributes": ["title", "overview"], "filterableAttributes": ["genres"], "stopWords": ["of", "the"] }, "error": null, "duration": "PT37.488777S", "enqueuedAt": "2021-09-08T08:24:02.323444Z", "startedAt": "2021-09-08T08:24:02.324145Z", "finishedAt": "2021-09-08T08:24:39.812922Z" }, { "uid": 0, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "error": null, "duration": "PT39.941318S", "enqueuedAt": "2021-09-08T08:21:14.742672Z", "startedAt": "2021-09-08T08:21:14.750166Z", "finishedAt": "2021-09-08T08:21:54.691484Z" }], "total": 2, "limit": 20, "from": 1, "next": null })
    );

    // finally we're just going to check that we can["results"] still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 100, "title": "Lock, Stock and Two Smoking Barrels", "genres": ["Comedy", "Crime"], "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000 })
    );

    let (document, code) = index.get_document(500, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 500, "title": "Reservoir Dogs", "genres": ["Crime", "Thriller"], "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 10006, "title": "Wild Seven", "genres": ["Action", "Crime", "Drama"], "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=indexUID&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
async fn import_dump_v3_rubygems_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::RubyGemsWithSettingsV3.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("rubygems"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("rubygems");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"description": 53, "id": 53, "name": 53, "summary": 53, "total_downloads": 53, "version": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
    {
      "displayedAttributes": [
        "name",
        "summary",
        "description",
        "version",
        "total_downloads"
      ],
      "searchableAttributes": [
        "name",
        "summary"
      ],
      "filterableAttributes": [
        "version"
      ],
      "sortableAttributes": [],
      "rankingRules": [
        "typo",
        "words",
        "fame:desc",
        "proximity",
        "attribute",
        "exactness",
        "total_downloads:desc"
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks["results"][0],
        json!({"uid": 92, "batchUid": null, "indexUid": "rubygems", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": {"receivedDocuments": 0, "indexedDocuments": 1042}, "error": null, "duration": "PT14.034672S", "enqueuedAt": "2021-09-08T08:40:31.390775Z", "startedAt": "2021-09-08T08:51:39.060642Z", "finishedAt": "2021-09-08T08:51:53.095314Z"})
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(188040, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "meilisearch", "summary": "An easy-to-use ruby client for Meilisearch API", "description": "An easy-to-use ruby client for Meilisearch API. See https://github.com/meilisearch/MeiliSearch", "id": "188040", "version": "0.15.2", "total_downloads": "7465"})
    );

    let (document, code) = index.get_document(191940, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "doggo", "summary": "RSpec 3 formatter - documentation, with progress indication", "description": "Similar to \"rspec -f d\", but also indicates progress by showing the current test number and total test count on each line.", "id": "191940", "version": "1.1.0", "total_downloads": "9394"})
    );

    let (document, code) = index.get_document(159227, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "vortex-of-agony", "summary": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "description": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "id": "159227", "version": "0.1.0", "total_downloads": "1007"})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=rubygems&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
async fn import_dump_v4_movie_raw() {
    let temp = tempfile::tempdir().unwrap();

    let options =
        Opt { import_dump: Some(GetDump::MoviesRawV4.path()), ..default_settings(temp.path()) };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks,
        json!({ "results": [{"uid": 0, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "error": null, "duration": "PT41.751156S", "enqueuedAt": "2021-09-08T08:30:30.550282Z", "startedAt": "2021-09-08T08:30:30.553012Z", "finishedAt": "2021-09-08T08:31:12.304168Z" }], "total": 1, "limit" : 20, "from": 0, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 100, "title": "Lock, Stock and Two Smoking Barrels", "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "genres": ["Comedy", "Crime"], "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000})
    );

    let (document, code) = index.get_document(500, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 500, "title": "Reservoir Dogs", "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "genres": ["Crime", "Thriller"], "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 10006, "title": "Wild Seven", "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "genres": ["Action", "Crime", "Drama"], "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=indexUID&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
async fn import_dump_v4_movie_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::MoviesWithSettingsV4.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("indexUID"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("indexUID");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"genres": 53, "id": 53, "overview": 53, "poster": 53, "release_date": 53, "title": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
    {
      "displayedAttributes": [
        "title",
        "genres",
        "overview",
        "poster",
        "release_date"
      ],
      "searchableAttributes": [
        "title",
        "overview"
      ],
      "filterableAttributes": [
        "genres"
      ],
      "sortableAttributes": [],
      "rankingRules": [
        "words",
        "typo",
        "proximity",
        "attribute",
        "exactness"
      ],
      "stopWords": [
        "of",
        "the"
      ],
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks,
        json!({ "results": [{ "uid": 1, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "settingsUpdate", "canceledBy": null, "details": { "displayedAttributes": ["title", "genres", "overview", "poster", "release_date"], "searchableAttributes": ["title", "overview"], "filterableAttributes": ["genres"], "stopWords": ["of", "the"] }, "error": null, "duration": "PT37.488777S", "enqueuedAt": "2021-09-08T08:24:02.323444Z", "startedAt": "2021-09-08T08:24:02.324145Z", "finishedAt": "2021-09-08T08:24:39.812922Z" }, { "uid": 0, "batchUid": null, "indexUid": "indexUID", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": { "receivedDocuments": 0, "indexedDocuments": 31944 }, "error": null, "duration": "PT39.941318S", "enqueuedAt": "2021-09-08T08:21:14.742672Z", "startedAt": "2021-09-08T08:21:14.750166Z", "finishedAt": "2021-09-08T08:21:54.691484Z" }], "total": 2, "limit": 20, "from": 1, "next": null })
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(100, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 100, "title": "Lock, Stock and Two Smoking Barrels", "genres": ["Comedy", "Crime"], "overview": "A card shark and his unwillingly-enlisted friends need to make a lot of cash quick after losing a sketchy poker match. To do this they decide to pull a heist on a small-time gang who happen to be operating out of the flat next door.", "poster": "https://image.tmdb.org/t/p/w500/8kSerJrhrJWKLk1LViesGcnrUPE.jpg", "release_date": 889056000 })
    );

    let (document, code) = index.get_document(500, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 500, "title": "Reservoir Dogs", "genres": ["Crime", "Thriller"], "overview": "A botched robbery indicates a police informant, and the pressure mounts in the aftermath at a warehouse. Crime begets violence as the survivors -- veteran Mr. White, newcomer Mr. Orange, psychopathic parolee Mr. Blonde, bickering weasel Mr. Pink and Nice Guy Eddie -- unravel.", "poster": "https://image.tmdb.org/t/p/w500/AjTtJNumZyUDz33VtMlF1K8JPsE.jpg", "release_date": 715392000})
    );

    let (document, code) = index.get_document(10006, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "id": 10006, "title": "Wild Seven", "genres": ["Action", "Crime", "Drama"], "overview": "In this darkly karmic vision of Arizona, a man who breathes nothing but ill will begins a noxious domino effect as quickly as an uncontrollable virus kills. As he exits Arizona State Penn after twenty-one long years, Wilson has only one thing on the brain, leveling the score with career criminal, Mackey Willis.", "poster": "https://image.tmdb.org/t/p/w500/y114dTPoqn8k2Txps4P2tI95YCS.jpg", "release_date": 1136073600})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=indexUID&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
async fn import_dump_v4_rubygems_with_settings() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::RubyGemsWithSettingsV4.path()),
        ..default_settings(temp.path())
    };
    let server = Server::new_with_options(options).await.unwrap();

    let (indexes, code) = server.list_indexes(None, None).await;
    snapshot!(code, @"200 OK");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("rubygems"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    let index = server.index("rubygems");

    let (stats, code) = index.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        stats,
        json!({ "numberOfDocuments": 53, "isIndexing": false, "fieldDistribution": {"description": 53, "id": 53, "name": 53, "summary": 53, "total_downloads": 53, "version": 53 }})
    );

    let (settings, code) = index.settings().await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(settings),
        @r###"
    {
      "displayedAttributes": [
        "name",
        "summary",
        "description",
        "version",
        "total_downloads"
      ],
      "searchableAttributes": [
        "name",
        "summary"
      ],
      "filterableAttributes": [
        "version"
      ],
      "sortableAttributes": [],
      "rankingRules": [
        "typo",
        "words",
        "fame:desc",
        "proximity",
        "attribute",
        "exactness",
        "total_downloads:desc"
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###
    );

    let (tasks, code) = index.list_tasks().await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        tasks["results"][0],
        json!({ "uid": 92, "batchUid": null, "indexUid": "rubygems", "status": "succeeded", "type": "documentAdditionOrUpdate", "canceledBy": null, "details": {"receivedDocuments": 0, "indexedDocuments": 1042}, "error": null, "duration": "PT14.034672S", "enqueuedAt": "2021-09-08T08:40:31.390775Z", "startedAt": "2021-09-08T08:51:39.060642Z", "finishedAt": "2021-09-08T08:51:53.095314Z"})
    );

    // finally we're just going to check that we can still get a few documents by id
    let (document, code) = index.get_document(188040, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "meilisearch", "summary": "An easy-to-use ruby client for Meilisearch API", "description": "An easy-to-use ruby client for Meilisearch API. See https://github.com/meilisearch/MeiliSearch", "id": "188040", "version": "0.15.2", "total_downloads": "7465"})
    );

    let (document, code) = index.get_document(191940, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "doggo", "summary": "RSpec 3 formatter - documentation, with progress indication", "description": "Similar to \"rspec -f d\", but also indicates progress by showing the current test number and total test count on each line.", "id": "191940", "version": "1.1.0", "total_downloads": "9394"})
    );

    let (document, code) = index.get_document(159227, None).await;
    snapshot!(code, @"200 OK");
    assert_eq!(
        document,
        json!({ "name": "vortex-of-agony", "summary": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "description": "You dont need to use nodejs or go, just install this plugin. It will crash your application at random", "id": "159227", "version": "0.1.0", "total_downloads": "1007"})
    );

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("indexUids=rubygems&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));
}

#[actix_rt::test]
async fn import_dump_v5() {
    let temp = tempfile::tempdir().unwrap();

    let options =
        Opt { import_dump: Some(GetDump::TestV5.path()), ..default_settings(temp.path()) };
    let mut server = Server::new_auth_with_options(options, temp).await;
    server.use_api_key("MASTER_KEY");

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200, "{indexes}");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 2);
    assert_eq!(indexes["results"][0]["uid"], json!("test"));
    assert_eq!(indexes["results"][1]["uid"], json!("test2"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

    // before doing anything we're going to wait until all the tasks in the dump have finished processing
    let result = server.tasks_filter("statuses=enqueued,processing").await.0;
    for task in result["results"].as_array().unwrap() {
        server.wait_task(task["uid"].as_u64().unwrap()).await;
    }

    let expected_stats = json!({
        "numberOfDocuments": 10,
        "isIndexing": false,
        "fieldDistribution": {
            "cast": 10,
            "director": 10,
            "genres": 10,
            "id": 10,
            "overview": 10,
            "popularity": 10,
            "poster_path": 10,
            "producer": 10,
            "production_companies": 10,
            "release_date": 10,
            "tagline": 10,
            "title": 10,
            "vote_average": 10,
            "vote_count": 10
        }
    });

    let index1 = server.index("test");
    let index2 = server.index("test2");

    let (stats, code) = index1.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(stats, expected_stats);

    let (docs, code) = index2.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"200 OK");
    assert_eq!(docs["results"].as_array().unwrap().len(), 10);
    let (docs, code) = index1.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"200 OK");
    assert_eq!(docs["results"].as_array().unwrap().len(), 10);

    let (stats, code) = index2.stats().await;
    snapshot!(code, @"200 OK");
    assert_eq!(stats, expected_stats);

    let (keys, code) = server.list_api_keys("").await;
    snapshot!(code, @"200 OK");
    let key = &keys["results"][0];

    assert_eq!(key["name"], "my key");

    // We're going to ensure that every reverse index of the task queue has been well built while importing the dump
    let (tasks, code) = server.tasks_filter("uids=0&limit=1&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("types=documentAdditionOrUpdate&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(tasks));

    let (tasks, code) = server.tasks_filter("statuses=succeeded&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(tasks, { ".results[].details.dumpUid" => "[uid]",  ".results[].duration" => "[duration]" ,  ".results[].startedAt" => "[date]" ,  ".results[].finishedAt" => "[date]"  })
    );

    let (tasks, code) = server.tasks_filter("indexUids=test&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(tasks, { ".results[].details.dumpUid" => "[uid]",  ".results[].duration" => "[duration]" ,  ".results[].startedAt" => "[date]" ,  ".results[].finishedAt" => "[date]"  })
    );

    let (tasks, code) = server.tasks_filter("afterEnqueuedAt=2021-09-05&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(tasks, { ".results[].details.dumpUid" => "[uid]",  ".results[].duration" => "[duration]" ,  ".results[].startedAt" => "[date]" ,  ".results[].finishedAt" => "[date]"  })
    );

    let (tasks, code) = server.tasks_filter("afterStartedAt=2021-09-06&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(tasks, { ".results[].details.dumpUid" => "[uid]",  ".results[].duration" => "[duration]" ,  ".results[].startedAt" => "[date]" ,  ".results[].finishedAt" => "[date]"  })
    );

    let (tasks, code) = server.tasks_filter("afterFinishedAt=2021-09-07&limit=1").await;
    snapshot!(code, @"200 OK");
    snapshot!(
        json_string!(tasks, { ".results[].details.dumpUid" => "[uid]",  ".results[].duration" => "[duration]" ,  ".results[].startedAt" => "[date]" ,  ".results[].finishedAt" => "[date]"  })
    );
}

#[actix_rt::test]
async fn import_dump_v6_containing_experimental_features() {
    let temp = tempfile::tempdir().unwrap();

    let options = Opt {
        import_dump: Some(GetDump::TestV6WithExperimental.path()),
        ..default_settings(temp.path())
    };
    let mut server = Server::new_auth_with_options(options, temp).await;
    server.use_api_key("MASTER_KEY");

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200, "{indexes}");

    assert_eq!(indexes["results"].as_array().unwrap().len(), 1);
    assert_eq!(indexes["results"][0]["uid"], json!("movies"));
    assert_eq!(indexes["results"][0]["primaryKey"], json!("id"));

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

    let index = server.index("movies");

    let (response, code) = index.settings().await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
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
        "proximity"
      ],
      "stopWords": [],
      "nonSeparatorTokens": [],
      "separatorTokens": [],
      "dictionary": [],
      "synonyms": {},
      "distinctAttribute": null,
      "proximityPrecision": "byAttribute",
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
      "embedders": {},
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###);

    // the expected order is [1, 3, 2] instead of [3, 1, 2]
    // because the attribute scale doesn't make the difference between 1 and 3.
    index
        .search(json!({"q": "the soup of day"}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"]), @r###"
            [
              {
                "id": 1,
                "a": "Soup of the day",
                "b": "many the fish"
              },
              {
                "id": 3,
                "a": "the Soup of day",
                "b": "many the fish"
              },
              {
                "id": 2,
                "a": "Soup of day",
                "b": "many the lazy fish"
              }
            ]
            "###);
        })
        .await;
}

// In this test we must generate the dump ourselves to ensure the
// `user provided` vectors are well set
#[actix_rt::test]
#[cfg_attr(target_os = "windows", ignore)]
async fn generate_and_import_dump_containing_vectors() {
    let temp = tempfile::tempdir().unwrap();
    let mut opt = default_settings(temp.path());
    let server = Server::new_with_options(opt.clone()).await.unwrap();

    let index = server.index("pets");
    let (response, code) = index
        .update_settings(json!(
        {
            "embedders": {
                "doggo_embedder": {
                    "source": "huggingFace",
                    "model": "sentence-transformers/all-MiniLM-L6-v2",
                    "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
                    "documentTemplate": "{{doc.doggo}}",
                }
            }
        }
        ))
        .await;
    snapshot!(code, @"202 Accepted");
    let response = index.wait_task(response.uid()).await;
    snapshot!(response);
    let (response, code) = index
        .add_documents(
            json!([
                {"id": 0, "doggo": "kefir", "_vectors": { "doggo_embedder": vec![0; 384] }},
                {"id": 1, "doggo": "echo", "_vectors": { "doggo_embedder": { "regenerate": false, "embeddings": vec![1; 384] }}},
                {"id": 2, "doggo": "intel", "_vectors": { "doggo_embedder": { "regenerate": true, "embeddings": vec![2; 384] }}},
                {"id": 3, "doggo": "bill", "_vectors": { "doggo_embedder": { "regenerate": true }}},
                {"id": 4, "doggo": "max" },
            ]),
            None,
        )
        .await;
    snapshot!(code, @"202 Accepted");
    let response = index.wait_task(response.uid()).await;
    snapshot!(response);

    let (response, code) = server.create_dump().await;
    snapshot!(code, @"202 Accepted");
    let response = index.wait_task(response.uid()).await;
    snapshot!(response["status"], @r###""succeeded""###);

    // ========= We made a dump, now we should clear the DB and try to import our dump
    drop(server);
    tokio::fs::remove_dir_all(&opt.db_path).await.unwrap();
    let dump_name = format!("{}.dump", response["details"]["dumpUid"].as_str().unwrap());
    let dump_path = opt.dump_dir.join(dump_name);
    assert!(dump_path.exists(), "path: `{}`", dump_path.display());

    opt.import_dump = Some(dump_path);
    // NOTE: We shouldn't have to change the database path but I lost one hour
    // because of a « bad path » error and that fixed it.
    opt.db_path = temp.path().join("data.ms");

    let mut server = Server::new_auth_with_options(opt, temp).await;
    server.use_api_key("MASTER_KEY");

    let (indexes, code) = server.list_indexes(None, None).await;
    assert_eq!(code, 200, "{indexes}");

    snapshot!(indexes["results"].as_array().unwrap().len(), @"1");
    snapshot!(indexes["results"][0]["uid"], @r###""pets""###);
    snapshot!(indexes["results"][0]["primaryKey"], @r###""id""###);

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

    let index = server.index("pets");

    let (response, code) = index.settings().await;
    meili_snap::snapshot!(code, @"200 OK");
    meili_snap::snapshot!(meili_snap::json_string!(response), @r###"
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
        "doggo_embedder": {
          "source": "huggingFace",
          "model": "sentence-transformers/all-MiniLM-L6-v2",
          "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
          "documentTemplate": "{{doc.doggo}}",
          "documentTemplateMaxBytes": 400
        }
      },
      "searchCutoffMs": null,
      "localizedAttributes": null,
      "facetSearch": true,
      "prefixSearch": "indexingTime"
    }
    "###);

    index
        .search(json!({"retrieveVectors": true}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(json_string!(response["hits"], { "[]._vectors.doggo_embedder.embeddings" => "[vector]" }), @r###"
            [
              {
                "id": 0,
                "doggo": "kefir",
                "_vectors": {
                  "doggo_embedder": {
                    "embeddings": "[vector]",
                    "regenerate": false
                  }
                }
              },
              {
                "id": 1,
                "doggo": "echo",
                "_vectors": {
                  "doggo_embedder": {
                    "embeddings": "[vector]",
                    "regenerate": false
                  }
                }
              },
              {
                "id": 2,
                "doggo": "intel",
                "_vectors": {
                  "doggo_embedder": {
                    "embeddings": "[vector]",
                    "regenerate": true
                  }
                }
              },
              {
                "id": 3,
                "doggo": "bill",
                "_vectors": {
                  "doggo_embedder": {
                    "embeddings": "[vector]",
                    "regenerate": true
                  }
                }
              },
              {
                "id": 4,
                "doggo": "max",
                "_vectors": {
                  "doggo_embedder": {
                    "embeddings": "[vector]",
                    "regenerate": true
                  }
                }
              }
            ]
            "###);
        })
        .await;
}
