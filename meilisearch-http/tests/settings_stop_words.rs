use assert_json_diff::assert_json_eq;use serde_json::json;mod common;#[actix_rt::test]async fn update_stop_words() {let mut server = common::Server::test_server().await;let (response, _status_code) = server.get_stop_words().await;assert_eq!(response.as_array().unwrap().is_empty(), true);let body = json!(["ut", "ea"]);server.update_stop_words(body.clone()).await;let (response, _status_code) = server.get_stop_words().await;assert_json_eq!(body, response, ordered: false);server.delete_stop_words().await;let (response, _status_code) = server.get_stop_words().await;assert_eq!(response.as_array().unwrap().is_empty(), true);}#[actix_rt::test]async fn add_documents_and_stop_words() {let mut server = common::Server::test_server().await;let body = json!(["ad", "in"]);server.update_stop_words(body.clone()).await;let (response, _status_code) = server.search_get("q=in%20exercitation").await;assert!(!response["hits"].as_array().unwrap().is_empty());let (response, _status_code) = server.search_get("q=ad%20in").await;assert!(response["hits"].as_array().unwrap().is_empty());}