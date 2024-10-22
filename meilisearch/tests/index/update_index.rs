use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::common::encoder::Encoder;
use crate::common::{
    shared_does_not_exists_index, Server,
};
use crate::json;

#[actix_rt::test]
async fn update_primary_key() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.create(None).await;

    assert_eq!(code, 202);

    index.update(Some("primary")).await;

    let response = index.wait_task(task.uid()).await.succeeded();

    assert_eq!(response["status"], "succeeded");

    let (response, code) = index.get().await;

    assert_eq!(code, 200);

    assert_eq!(response["uid"], "test");
    assert!(response.get("createdAt").is_some());
    assert!(response.get("updatedAt").is_some());

    let created_at =
        OffsetDateTime::parse(response["createdAt"].as_str().unwrap(), &Rfc3339).unwrap();
    let updated_at =
        OffsetDateTime::parse(response["updatedAt"].as_str().unwrap(), &Rfc3339).unwrap();
    assert!(created_at < updated_at);

    assert_eq!(response["primaryKey"], "primary");
    assert_eq!(response.as_object().unwrap().len(), 4);
}

#[actix_rt::test]
async fn create_and_update_with_different_encoding() {
    let server = Server::new_shared();
    let index = server.index_with_encoder("test", Encoder::Gzip);
    let (task, code) = index.create(None).await;

    assert_eq!(code, 202);

    let index = server.index_with_encoder("test", Encoder::Brotli);
    index.update(Some("primary")).await;

    let response = index.wait_task(task.uid()).await.succeeded();

    assert_eq!(response["status"], "succeeded");
}

#[actix_rt::test]
async fn update_nothing() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.create(None).await;

    assert_eq!(code, 202);

    index.wait_task(task.uid()).await.succeeded();

    let (task, code) = index.update(None).await;

    assert_eq!(code, 202);

    let response = index.wait_task(task.uid()).await.succeeded();

    assert_eq!(response["status"], "succeeded");
}

#[actix_rt::test]
async fn error_update_existing_primary_key() {
    let server = Server::new_shared();
    let index = server.unique_index();
    let (task, code) = index.create(Some("id")).await;

    assert_eq!(code, 202);

    let documents = json!([
        {
            "id": "11",
            "content": "foobar"
        }
    ]);
    index.add_documents(documents, None).await;

    let (task, code) = index.update(Some("primary")).await;

    assert_eq!(code, 202);

    let response = index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": "Index already has a primary key: `id`.",
        "code": "index_primary_key_already_exists",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_primary_key_already_exists"
    });

    assert_eq!(response["error"], expected_response);
}

#[actix_rt::test]
async fn error_update_unexisting_index() {
    let server = Server::new_shared();
    let index = shared_does_not_exists_index();
    let (task, code) = index.update(None).await;

    assert_eq!(code, 202);

    let response = index.wait_task(task.uid()).await.succeeded();

    let expected_response = json!({
        "message": "Index `DOES_NOT_EXISTS` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response["error"], expected_response);
}
