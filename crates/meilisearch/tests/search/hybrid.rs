use meili_snap::snapshot;
use once_cell::sync::Lazy;

use crate::common::index::Index;
use crate::common::{Server, Value};
use crate::json;

async fn index_with_documents_user_provided<'a>(
    server: &'a Server,
    documents: &Value,
) -> Index<'a> {
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({ "embedders": {"default": {
                "source": "userProvided",
                "dimensions": 2}}} ))
        .await;
    assert_eq!(202, code, "{:?}", response);
    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.add_documents(documents.clone(), None).await;
    assert_eq!(202, code, "{:?}", response);
    index.wait_task(response.uid()).await.succeeded();
    index
}

async fn index_with_documents_hf<'a>(server: &'a Server, documents: &Value) -> Index<'a> {
    let index = server.index("test");

    let (response, code) = index
        .update_settings(json!({ "embedders": {"default": {
            "source": "huggingFace",
            "model": "sentence-transformers/all-MiniLM-L6-v2",
            "revision": "e4ce9877abf3edfe10b0d82785e83bdcb973e22e",
            "documentTemplate": "{{doc.title}}, {{doc.desc}}"
        }}} ))
        .await;
    assert_eq!(202, code, "{:?}", response);
    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.add_documents(documents.clone(), None).await;
    assert_eq!(202, code, "{:?}", response);
    index.wait_task(response.uid()).await.succeeded();
    index
}

static SIMPLE_SEARCH_DOCUMENTS_VEC: Lazy<Value> = Lazy::new(|| {
    json!([
    {
        "title": "Shazam!",
        "desc": "a Captain Marvel ersatz",
        "id": "1",
        "_vectors": {"default": [1.0, 3.0]},
    },
    {
        "title": "Captain Planet",
        "desc": "He's not part of the Marvel Cinematic Universe",
        "id": "2",
        "_vectors": {"default": [1.0, 2.0]},
    },
    {
        "title": "Captain Marvel",
        "desc": "a Shazam ersatz",
        "id": "3",
        "_vectors": {"default": [2.0, 3.0]},
    }])
});

static SINGLE_DOCUMENT_VEC: Lazy<Value> = Lazy::new(|| {
    json!([{
            "title": "Shazam!",
            "desc": "a Captain Marvel ersatz",
            "id": "1",
            "_vectors": {"default": [1.0, 3.0]},
    }])
});

static SIMPLE_SEARCH_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
    {
        "title": "Shazam!",
        "desc": "a Captain Marvel ersatz",
        "id": "1",
    },
    {
        "title": "Captain Planet",
        "desc": "He's not part of the Marvel Cinematic Universe",
        "id": "2",
    },
    {
        "title": "Captain Marvel",
        "desc": "a Shazam ersatz",
        "id": "3",
    }])
});

#[actix_rt::test]
async fn simple_search() {
    let server = Server::new().await;
    let index = index_with_documents_user_provided(&server, &SIMPLE_SEARCH_DOCUMENTS_VEC).await;

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "vector": [1.0, 1.0], "hybrid": {"semanticRatio": 0.2, "embedder": "default"}, "retrieveVectors": true}),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}}},{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}}},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}}}]"###);
    snapshot!(response["semanticHitCount"], @"0");

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "vector": [1.0, 1.0], "hybrid": {"semanticRatio": 0.5, "embedder": "default"}, "showRankingScore": true, "retrieveVectors": true}),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_rankingScore":0.990290343761444},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_rankingScore":0.9848484848484848},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_rankingScore":0.9472135901451112}]"###);
    snapshot!(response["semanticHitCount"], @"2");

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "vector": [1.0, 1.0], "hybrid": {"semanticRatio": 0.8, "embedder": "default"}, "showRankingScore": true, "retrieveVectors": true}),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_rankingScore":0.990290343761444},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_rankingScore":0.974341630935669},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_rankingScore":0.9472135901451112}]"###);
    snapshot!(response["semanticHitCount"], @"3");
}

#[actix_rt::test]
async fn limit_offset() {
    let server = Server::new().await;
    let index = index_with_documents_user_provided(&server, &SIMPLE_SEARCH_DOCUMENTS_VEC).await;

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "vector": [1.0, 1.0], "hybrid": {"semanticRatio": 0.2, "embedder": "default"}, "retrieveVectors": true, "offset": 1, "limit": 1}),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}}}]"###);
    snapshot!(response["semanticHitCount"], @"0");
    assert_eq!(response["hits"].as_array().unwrap().len(), 1);

    let server = Server::new().await;
    let index = index_with_documents_user_provided(&server, &SIMPLE_SEARCH_DOCUMENTS_VEC).await;

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "vector": [1.0, 1.0], "hybrid": {"semanticRatio": 0.9, "embedder": "default"}, "retrieveVectors": true, "offset": 1, "limit": 1}),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}}}]"###);
    snapshot!(response["semanticHitCount"], @"1");
    assert_eq!(response["hits"].as_array().unwrap().len(), 1);
}

#[actix_rt::test]
async fn simple_search_hf() {
    let server = Server::new().await;
    let index = index_with_documents_hf(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "hybrid": {"semanticRatio": 0.2, "embedder": "default"}}),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2"},{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3"},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1"}]"###);
    snapshot!(response["semanticHitCount"], @"0");

    let (response, code) = index
        .search_post(
            // disable ranking score as the vectors between architectures are not equal
            json!({"q": "Captain", "hybrid": {"embedder": "default", "semanticRatio": 0.55}, "showRankingScore": false}),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2"},{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3"},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1"}]"###);
    snapshot!(response["semanticHitCount"], @"1");

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "hybrid": {"embedder": "default", "semanticRatio": 0.8}, "showRankingScore": false}),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1"},{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3"},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2"}]"###);
    snapshot!(response["semanticHitCount"], @"3");

    let (response, code) = index
        .search_post(
            json!({"q": "Movie World", "hybrid": {"embedder": "default", "semanticRatio": 0.2}, "showRankingScore": false}),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2"},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1"},{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3"}]"###);
    snapshot!(response["semanticHitCount"], @"3");

    let (response, code) = index
        .search_post(
            json!({"q": "Wonder replacement", "hybrid": {"embedder": "default", "semanticRatio": 0.2}, "showRankingScore": false}),
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3"},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1"},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2"}]"###);
    snapshot!(response["semanticHitCount"], @"3");
}

#[actix_rt::test]
async fn distribution_shift() {
    let server = Server::new().await;
    let index = index_with_documents_user_provided(&server, &SIMPLE_SEARCH_DOCUMENTS_VEC).await;

    let search = json!({"q": "Captain", "vector": [1.0, 1.0], "showRankingScore": true, "hybrid": {"embedder": "default", "semanticRatio": 1.0}, "retrieveVectors": true});
    let (response, code) = index.search_post(search.clone()).await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_rankingScore":0.990290343761444},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_rankingScore":0.974341630935669},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_rankingScore":0.9472135901451112}]"###);

    let (response, code) = index
        .update_settings(json!({
            "embedders": {
                "default": {
                    "distribution": {
                        "mean": 0.998,
                        "sigma": 0.01
                    }
                }
            }
        }))
        .await;

    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;
    snapshot!(response["details"], @r#"{"embedders":{"default":{"distribution":{"mean":0.998,"sigma":0.01}}}}"#);

    let (response, code) = index.search_post(search).await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_rankingScore":0.19161224365234375},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_rankingScore":1.1920928955078125e-7},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_rankingScore":1.1920928955078125e-7}]"###);
}

#[actix_rt::test]
async fn highlighter() {
    let server = Server::new().await;
    let index = index_with_documents_user_provided(&server, &SIMPLE_SEARCH_DOCUMENTS_VEC).await;

    let (response, code) = index
        .search_post(json!({"q": "Captain Marvel", "vector": [1.0, 1.0],
            "hybrid": {"embedder": "default", "semanticRatio": 0.2},
           "retrieveVectors": true,
           "attributesToHighlight": [
                     "desc",
                     "_vectors",
                   ],
           "highlightPreTag": "**BEGIN**",
           "highlightPostTag": "**END**",
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_formatted":{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3"}},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_formatted":{"title":"Shazam!","desc":"a **BEGIN**Captain**END** **BEGIN**Marvel**END** ersatz","id":"1"}},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_formatted":{"title":"Captain Planet","desc":"He's not part of the **BEGIN**Marvel**END** Cinematic Universe","id":"2"}}]"###);
    snapshot!(response["semanticHitCount"], @"0");

    let (response, code) = index
        .search_post(json!({"q": "Captain Marvel", "vector": [1.0, 1.0],
            "hybrid": {"embedder": "default", "semanticRatio": 0.8},
            "retrieveVectors": true,
            "showRankingScore": true,
            "attributesToHighlight": [
                     "desc"
                   ],
                   "highlightPreTag": "**BEGIN**",
                   "highlightPostTag": "**END**"
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_formatted":{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3"},"_rankingScore":0.990290343761444},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_formatted":{"title":"Captain Planet","desc":"He's not part of the **BEGIN**Marvel**END** Cinematic Universe","id":"2"},"_rankingScore":0.974341630935669},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_formatted":{"title":"Shazam!","desc":"a **BEGIN**Captain**END** **BEGIN**Marvel**END** ersatz","id":"1"},"_rankingScore":0.9472135901451112}]"###);
    snapshot!(response["semanticHitCount"], @"3");

    // no highlighting on full semantic
    let (response, code) = index
        .search_post(json!({"q": "Captain Marvel", "vector": [1.0, 1.0],
            "hybrid": {"embedder": "default", "semanticRatio": 1.0},
            "retrieveVectors": true,
            "showRankingScore": true,
            "attributesToHighlight": [
                     "desc"
                   ],
                   "highlightPreTag": "**BEGIN**",
                   "highlightPostTag": "**END**"
        }))
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_formatted":{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3"},"_rankingScore":0.990290343761444},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_formatted":{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2"},"_rankingScore":0.974341630935669},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_formatted":{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1"},"_rankingScore":0.9472135901451112}]"###);
    snapshot!(response["semanticHitCount"], @"3");
}

#[actix_rt::test]
async fn invalid_semantic_ratio() {
    let server = Server::new().await;
    let index = index_with_documents_user_provided(&server, &SIMPLE_SEARCH_DOCUMENTS_VEC).await;

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "vector": [1.0, 1.0], "hybrid": {"embedder": "default", "semanticRatio": 1.2}}),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value at `.hybrid.semanticRatio`: the value of `semanticRatio` is invalid, expected a float between `0.0` and `1.0`.",
      "code": "invalid_search_semantic_ratio",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_semantic_ratio"
    }
    "###);

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "vector": [1.0, 1.0], "hybrid": {"embedder": "default", "semanticRatio": -0.8}}),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value at `.hybrid.semanticRatio`: the value of `semanticRatio` is invalid, expected a float between `0.0` and `1.0`.",
      "code": "invalid_search_semantic_ratio",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_semantic_ratio"
    }
    "###);

    let (response, code) = index
        .search_get(
            &yaup::to_string(
                &json!({"q": "Captain", "vector": [1.0, 1.0], "hybridEmbedder": "default", "hybridSemanticRatio": 1.2}),
            )
            .unwrap(),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value in parameter `hybridSemanticRatio`: the value of `semanticRatio` is invalid, expected a float between `0.0` and `1.0`.",
      "code": "invalid_search_semantic_ratio",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_semantic_ratio"
    }
    "###);

    let (response, code) = index
        .search_get(
            &yaup::to_string(
                &json!({"q": "Captain", "vector": [1.0, 1.0], "hybridEmbedder": "default", "hybridSemanticRatio": -0.2}),
            )
            .unwrap(),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value in parameter `hybridSemanticRatio`: the value of `semanticRatio` is invalid, expected a float between `0.0` and `1.0`.",
      "code": "invalid_search_semantic_ratio",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_search_semantic_ratio"
    }
    "###);
}

#[actix_rt::test]
async fn single_document() {
    let server = Server::new().await;
    let index = index_with_documents_user_provided(&server, &SINGLE_DOCUMENT_VEC).await;

    let (response, code) = index
    .search_post(
        json!({"vector": [1.0, 3.0], "hybrid": {"semanticRatio": 1.0, "embedder": "default"}, "showRankingScore": true, "retrieveVectors": true}),
    )
    .await;

    snapshot!(code, @"200 OK");
    snapshot!(response["hits"][0], @r###"{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_rankingScore":1.0}"###);
    snapshot!(response["semanticHitCount"], @"1");
}

#[actix_rt::test]
async fn query_combination() {
    let server = Server::new().await;
    let index = index_with_documents_user_provided(&server, &SIMPLE_SEARCH_DOCUMENTS_VEC).await;

    // search without query and vector, but with hybrid => still placeholder
    let (response, code) = index
        .search_post(json!({"hybrid": {"embedder": "default", "semanticRatio": 1.0}, "showRankingScore": true, "retrieveVectors": true}))
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_rankingScore":1.0},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_rankingScore":1.0},{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_rankingScore":1.0}]"###);
    snapshot!(response["semanticHitCount"], @"null");

    // same with a different semantic ratio
    let (response, code) = index
        .search_post(json!({"hybrid": {"embedder": "default", "semanticRatio": 0.76}, "showRankingScore": true, "retrieveVectors": true}))
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_rankingScore":1.0},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_rankingScore":1.0},{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_rankingScore":1.0}]"###);
    snapshot!(response["semanticHitCount"], @"null");

    // wrong vector dimensions
    let (response, code) = index
    .search_post(json!({"vector": [1.0, 0.0, 1.0], "hybrid": {"embedder": "default", "semanticRatio": 1.0}, "showRankingScore": true, "retrieveVectors": true}))
    .await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid vector dimensions: expected: `2`, found: `3`.",
      "code": "invalid_vector_dimensions",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_vector_dimensions"
    }
    "###);

    // full vector
    let (response, code) = index
    .search_post(json!({"vector": [1.0, 0.0], "hybrid": {"embedder": "default", "semanticRatio": 1.0}, "showRankingScore": true, "retrieveVectors": true}))
    .await;

    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_rankingScore":0.7773500680923462},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_rankingScore":0.7236068248748779},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_rankingScore":0.6581138968467712}]"###);
    snapshot!(response["semanticHitCount"], @"3");

    // full keyword, without a query
    let (response, code) = index
    .search_post(json!({"vector": [1.0, 0.0], "hybrid": {"embedder": "default", "semanticRatio": 0.0}, "showRankingScore": true, "retrieveVectors": true}))
    .await;

    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_rankingScore":1.0},{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_rankingScore":1.0},{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_rankingScore":1.0}]"###);
    snapshot!(response["semanticHitCount"], @"null");

    // query + vector, full keyword => keyword
    let (response, code) = index
    .search_post(json!({"q": "Captain", "vector": [1.0, 0.0], "hybrid": {"embedder": "default", "semanticRatio": 0.0}, "showRankingScore": true, "retrieveVectors": true}))
    .await;

    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_rankingScore":0.9848484848484848},{"title":"Captain Marvel","desc":"a Shazam ersatz","id":"3","_vectors":{"default":{"embeddings":[[2.0,3.0]],"regenerate":false}},"_rankingScore":0.9848484848484848},{"title":"Shazam!","desc":"a Captain Marvel ersatz","id":"1","_vectors":{"default":{"embeddings":[[1.0,3.0]],"regenerate":false}},"_rankingScore":0.9242424242424242}]"###);
    snapshot!(response["semanticHitCount"], @"null");

    // query + vector, no hybrid keyword =>
    let (response, code) = index
        .search_post(json!({"q": "Captain", "vector": [1.0, 0.0], "showRankingScore": true, "retrieveVectors": true}))
        .await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid request: missing `hybrid` parameter when `vector` is present.",
      "code": "missing_search_hybrid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_search_hybrid"
    }
    "###);

    // full vector, without a vector => error
    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "hybrid": {"semanticRatio": 1.0, "embedder": "default"}, "showRankingScore": true, "retrieveVectors": true}),
        )
        .await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Error while generating embeddings: user error: attempt to embed the following text in a configuration where embeddings must be user provided:\n  - `Captain`",
      "code": "vector_embedding_error",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#vector_embedding_error"
    }
    "###);

    // hybrid without a vector => full keyword
    let (response, code) = index
        .search_post(
            json!({"q": "Planet", "hybrid": {"semanticRatio": 0.99, "embedder": "default"}, "showRankingScore": true, "retrieveVectors": true}),
        )
        .await;

    snapshot!(code, @"200 OK");
    snapshot!(response["hits"], @r###"[{"title":"Captain Planet","desc":"He's not part of the Marvel Cinematic Universe","id":"2","_vectors":{"default":{"embeddings":[[1.0,2.0]],"regenerate":false}},"_rankingScore":0.9242424242424242}]"###);
    snapshot!(response["semanticHitCount"], @"0");
}

#[actix_rt::test]
async fn retrieve_vectors() {
    let server = Server::new().await;
    let index = index_with_documents_hf(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "hybrid": {"embedder": "default", "semanticRatio": 0.2}, "retrieveVectors": true}),
        )
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response["hits"], {"[]._vectors.default.embeddings" => "[vectors]"},  @r###"
    [
      {
        "title": "Captain Planet",
        "desc": "He's not part of the Marvel Cinematic Universe",
        "id": "2",
        "_vectors": {
          "default": {
            "embeddings": "[vectors]",
            "regenerate": true
          }
        }
      },
      {
        "title": "Captain Marvel",
        "desc": "a Shazam ersatz",
        "id": "3",
        "_vectors": {
          "default": {
            "embeddings": "[vectors]",
            "regenerate": true
          }
        }
      },
      {
        "title": "Shazam!",
        "desc": "a Captain Marvel ersatz",
        "id": "1",
        "_vectors": {
          "default": {
            "embeddings": "[vectors]",
            "regenerate": true
          }
        }
      }
    ]
    "###);

    // use explicit `_vectors` in displayed attributes
    let (response, code) = index
        .update_settings(json!({ "displayedAttributes": ["id", "title", "desc", "_vectors"]} ))
        .await;
    assert_eq!(202, code, "{:?}", response);
    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "hybrid": {"embedder": "default", "semanticRatio": 0.2}, "retrieveVectors": true}),
        )
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response["hits"], {"[]._vectors.default.embeddings" => "[vectors]"},  @r###"
    [
      {
        "title": "Captain Planet",
        "desc": "He's not part of the Marvel Cinematic Universe",
        "id": "2",
        "_vectors": {
          "default": {
            "embeddings": "[vectors]",
            "regenerate": true
          }
        }
      },
      {
        "title": "Captain Marvel",
        "desc": "a Shazam ersatz",
        "id": "3",
        "_vectors": {
          "default": {
            "embeddings": "[vectors]",
            "regenerate": true
          }
        }
      },
      {
        "title": "Shazam!",
        "desc": "a Captain Marvel ersatz",
        "id": "1",
        "_vectors": {
          "default": {
            "embeddings": "[vectors]",
            "regenerate": true
          }
        }
      }
    ]
    "###);

    // remove `_vectors` from displayed attributes
    let (response, code) =
        index.update_settings(json!({ "displayedAttributes": ["id", "title", "desc"]} )).await;
    assert_eq!(202, code, "{:?}", response);
    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index
        .search_post(
            json!({"q": "Captain", "hybrid": {"embedder": "default", "semanticRatio": 0.2}, "retrieveVectors": true}),
        )
        .await;
    snapshot!(code, @"200 OK");
    insta::assert_json_snapshot!(response["hits"], {"[]._vectors.default.embeddings" => "[vectors]"},  @r###"
    [
      {
        "title": "Captain Planet",
        "desc": "He's not part of the Marvel Cinematic Universe",
        "id": "2"
      },
      {
        "title": "Captain Marvel",
        "desc": "a Shazam ersatz",
        "id": "3"
      },
      {
        "title": "Shazam!",
        "desc": "a Captain Marvel ersatz",
        "id": "1"
      }
    ]
    "###);
}
