use crate::common::{
    shared_does_not_exists_index, Server,
};

use crate::json;

#[actix_rt::test]
async fn stats() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.create(Some("id")).await;

    assert_eq!(code, 202);

    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.stats().await;

    assert_eq!(code, 200);
    assert_eq!(response["numberOfDocuments"], 0);
    assert!(response["isIndexing"] == false);
    assert!(response["fieldDistribution"].as_object().unwrap().is_empty());

    let documents = json!([
        {
            "id": 1,
            "name": "Alexey",
        },
        {
            "id": 2,
            "age": 45,
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    assert_eq!(response["taskUid"], 1);

    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.stats().await;

    assert_eq!(code, 200);
    assert_eq!(response["numberOfDocuments"], 2);
    assert!(response["isIndexing"] == false);
    assert_eq!(response["fieldDistribution"]["id"], 2);
    assert_eq!(response["fieldDistribution"]["name"], 1);
    assert_eq!(response["fieldDistribution"]["age"], 1);
}

#[actix_rt::test]
async fn error_get_stats_unexisting_index() {
    let index = shared_does_not_exists_index();
    let (response, code) = index.stats().await;

    let expected_response = json!({
        "message": "Index `DOES_NOT_EXISTS` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response, expected_response);
    assert_eq!(code, 404);
}
