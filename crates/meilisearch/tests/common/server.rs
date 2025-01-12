#![allow(dead_code)]

use std::marker::PhantomData;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use actix_http::body::MessageBody;
use actix_web::dev::ServiceResponse;
use actix_web::http::StatusCode;
use byte_unit::{Byte, Unit};
use clap::Parser;
use meilisearch::option::{IndexerOpts, MaxMemory, MaxThreads, Opt};
use meilisearch::setup_meilisearch;
use once_cell::sync::Lazy;
use tempfile::TempDir;
use tokio::sync::OnceCell;
use tokio::time::sleep;
use uuid::Uuid;

use super::index::Index;
use super::service::Service;
use super::{Owned, Shared};
use crate::common::encoder::Encoder;
use crate::common::Value;
use crate::json;

pub struct Server<State = Owned> {
    pub service: Service,
    // hold ownership to the tempdir while we use the server instance.
    _dir: Option<TempDir>,
    _marker: PhantomData<State>,
}

pub static TEST_TEMP_DIR: Lazy<TempDir> = Lazy::new(|| TempDir::new().unwrap());

impl Server<Owned> {
    fn into_shared(self) -> Server<Shared> {
        Server { service: self.service, _dir: self._dir, _marker: PhantomData }
    }

    pub async fn new() -> Self {
        let dir = TempDir::new().unwrap();

        if cfg!(windows) {
            std::env::set_var("TMP", TEST_TEMP_DIR.path());
        } else {
            std::env::set_var("TMPDIR", TEST_TEMP_DIR.path());
        }

        let options = default_settings(dir.path());

        let (index_scheduler, auth) = setup_meilisearch(&options).unwrap();
        let service = Service { index_scheduler, auth, options, api_key: None };

        Server { service, _dir: Some(dir), _marker: PhantomData }
    }

    pub async fn new_auth_with_options(mut options: Opt, dir: TempDir) -> Self {
        if cfg!(windows) {
            std::env::set_var("TMP", TEST_TEMP_DIR.path());
        } else {
            std::env::set_var("TMPDIR", TEST_TEMP_DIR.path());
        }

        options.master_key = Some("MASTER_KEY".to_string());

        let (index_scheduler, auth) = setup_meilisearch(&options).unwrap();
        let service = Service { index_scheduler, auth, options, api_key: None };

        Server { service, _dir: Some(dir), _marker: PhantomData }
    }

    pub async fn new_auth() -> Self {
        let dir = TempDir::new().unwrap();
        let options = default_settings(dir.path());
        Self::new_auth_with_options(options, dir).await
    }

    pub async fn new_with_options(options: Opt) -> Result<Self, anyhow::Error> {
        let (index_scheduler, auth) = setup_meilisearch(&options)?;
        let service = Service { index_scheduler, auth, options, api_key: None };

        Ok(Server { service, _dir: None, _marker: PhantomData })
    }

    pub fn use_api_key(&mut self, api_key: impl AsRef<str>) {
        self.service.api_key = Some(api_key.as_ref().to_string());
    }

    /// Fetch and use the default admin key for nexts http requests.
    pub async fn use_admin_key(&mut self, master_key: impl AsRef<str>) {
        self.use_api_key(master_key);
        let (response, code) = self.list_api_keys("").await;
        assert_eq!(200, code, "{:?}", response);
        let admin_key = &response["results"][1]["key"];
        self.use_api_key(admin_key.as_str().unwrap());
    }

    pub async fn add_api_key(&self, content: Value) -> (Value, StatusCode) {
        let url = "/keys";
        self.service.post(url, content).await
    }

    pub async fn patch_api_key(&self, key: impl AsRef<str>, content: Value) -> (Value, StatusCode) {
        let url = format!("/keys/{}", key.as_ref());
        self.service.patch(url, content).await
    }

    pub async fn delete_api_key(&self, key: impl AsRef<str>) -> (Value, StatusCode) {
        let url = format!("/keys/{}", key.as_ref());
        self.service.delete(url).await
    }

    /// Returns a view to an index. There is no guarantee that the index exists.
    pub fn index(&self, uid: impl AsRef<str>) -> Index<'_> {
        self.index_with_encoder(uid, Encoder::Plain)
    }

    pub async fn create_index(&self, body: Value) -> (Value, StatusCode) {
        self.service.post("/indexes", body).await
    }

    pub fn index_with_encoder(&self, uid: impl AsRef<str>, encoder: Encoder) -> Index<'_> {
        Index {
            uid: uid.as_ref().to_string(),
            service: &self.service,
            encoder,
            marker: PhantomData,
        }
    }

    pub async fn list_indexes(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> (Value, StatusCode) {
        let (offset, limit) = (
            offset.map(|offset| format!("offset={offset}")),
            limit.map(|limit| format!("limit={limit}")),
        );
        let query_parameter = offset
            .as_ref()
            .zip(limit.as_ref())
            .map(|(offset, limit)| format!("{offset}&{limit}"))
            .or_else(|| offset.xor(limit));
        if let Some(query_parameter) = query_parameter {
            self.service.get(format!("/indexes?{query_parameter}")).await
        } else {
            self.service.get("/indexes").await
        }
    }

    pub async fn stats(&self) -> (Value, StatusCode) {
        self.service.get("/stats").await
    }

    pub async fn tasks(&self) -> (Value, StatusCode) {
        self.service.get("/tasks").await
    }

    pub async fn set_features(&self, value: Value) -> (Value, StatusCode) {
        self.service.patch("/experimental-features", value).await
    }

    pub async fn get_metrics(&self) -> (Value, StatusCode) {
        self.service.get("/metrics").await
    }
}

impl Server<Shared> {
    fn init_new_shared_instance() -> Server<Shared> {
        let dir = TempDir::new().unwrap();

        if cfg!(windows) {
            std::env::set_var("TMP", TEST_TEMP_DIR.path());
        } else {
            std::env::set_var("TMPDIR", TEST_TEMP_DIR.path());
        }

        let options = default_settings(dir.path());

        let (index_scheduler, auth) = setup_meilisearch(&options).unwrap();
        let service = Service { index_scheduler, auth, api_key: None, options };

        Server { service, _dir: Some(dir), _marker: PhantomData }
    }

    pub fn new_shared() -> &'static Server<Shared> {
        static SERVER: Lazy<Server<Shared>> = Lazy::new(Server::init_new_shared_instance);
        &SERVER
    }

    pub async fn new_shared_with_admin_key() -> &'static Server<Shared> {
        static SERVER: OnceCell<Server<Shared>> = OnceCell::const_new();
        SERVER
            .get_or_init(|| async {
                let mut server = Server::new_auth().await;
                server.use_admin_key("MASTER_KEY").await;
                server.into_shared()
            })
            .await
    }

    /// You shouldn't access random indexes on a shared instance thus this method
    /// must fail.
    pub async fn get_index_fail(&self, uid: impl AsRef<str>) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", urlencoding::encode(uid.as_ref()));
        let (value, code) = self.service.get(url).await;
        if code.is_success() {
            panic!("`get_index_fail` succeeded with uid: {}", uid.as_ref());
        }
        (value, code)
    }

    pub async fn delete_index_fail(&self, uid: impl AsRef<str>) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", urlencoding::encode(uid.as_ref()));
        let (value, code) = self.service.delete(url).await;
        if code.is_success() {
            panic!("`delete_index_fail` succeeded with uid: {}", uid.as_ref());
        }
        (value, code)
    }

    pub async fn list_indexes(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> (Value, StatusCode) {
        let (offset, limit) = (
            offset.map(|offset| format!("offset={offset}")),
            limit.map(|limit| format!("limit={limit}")),
        );
        let query_parameter = offset
            .as_ref()
            .zip(limit.as_ref())
            .map(|(offset, limit)| format!("{offset}&{limit}"))
            .or_else(|| offset.xor(limit));
        if let Some(query_parameter) = query_parameter {
            self.service.get(format!("/indexes?{query_parameter}")).await
        } else {
            self.service.get("/indexes").await
        }
    }
    pub async fn update_raw_index_fail(
        &self,
        uid: impl AsRef<str>,
        body: Value,
    ) -> (Value, StatusCode) {
        let url = format!("/indexes/{}", urlencoding::encode(uid.as_ref()));
        let (value, code) = self.service.patch_encoded(url, body, Encoder::Plain).await;
        if code.is_success() {
            panic!("`update_raw_index_fail` succeeded with uid: {}", uid.as_ref());
        }
        (value, code)
    }

    /// Since this call updates the state of the instance, it must fail.
    /// If it doesn't fail, the test will panic to help you debug what
    /// is going on.
    pub async fn create_index_fail(&self, body: Value) -> (Value, StatusCode) {
        let (mut task, code) = self._create_index(body).await;
        if code.is_success() {
            task = self.wait_task(task.uid()).await;
            if task.is_success() {
                panic!(
                    "`create_index_fail` succeeded: {}",
                    serde_json::to_string_pretty(&task).unwrap()
                );
            }
        }
        (task, code)
    }
}

impl<State> Server<State> {
    pub async fn init_web_app(
        &self,
    ) -> impl actix_web::dev::Service<
        actix_http::Request,
        Response = ServiceResponse<impl MessageBody>,
        Error = actix_web::Error,
    > {
        self.service.init_web_app().await
    }

    pub async fn list_api_keys(&self, params: &str) -> (Value, StatusCode) {
        let url = format!("/keys{params}");
        self.service.get(url).await
    }

    pub async fn dummy_request(
        &self,
        method: impl AsRef<str>,
        url: impl AsRef<str>,
    ) -> (Value, StatusCode) {
        match method.as_ref() {
            "POST" => self.service.post(url, json!({})).await,
            "PUT" => self.service.put(url, json!({})).await,
            "PATCH" => self.service.patch(url, json!({})).await,
            "GET" => self.service.get(url).await,
            "DELETE" => self.service.delete(url).await,
            _ => unreachable!(),
        }
    }

    pub async fn get_api_key(&self, key: impl AsRef<str>) -> (Value, StatusCode) {
        let url = format!("/keys/{}", key.as_ref());
        self.service.get(url).await
    }

    pub(super) fn _index(&self, uid: impl AsRef<str>) -> Index<'_> {
        Index {
            uid: uid.as_ref().to_string(),
            service: &self.service,
            encoder: Encoder::Plain,
            marker: PhantomData,
        }
    }

    /// Returns a view to an index. There is no guarantee that the index exists.
    pub fn unique_index(&self) -> Index<'_> {
        let uuid = Uuid::new_v4();
        Index {
            uid: uuid.to_string(),
            service: &self.service,
            encoder: Encoder::Plain,
            marker: PhantomData,
        }
    }

    pub fn unique_index_with_encoder(&self, encoder: Encoder) -> Index<'_> {
        let uuid = Uuid::new_v4();
        Index { uid: uuid.to_string(), service: &self.service, encoder, marker: PhantomData }
    }

    pub(super) async fn _create_index(&self, body: Value) -> (Value, StatusCode) {
        self.service.post("/indexes", body).await
    }

    pub async fn multi_search(&self, queries: Value) -> (Value, StatusCode) {
        self.service.post("/multi-search", queries).await
    }

    pub async fn list_indexes_raw(&self, parameters: &str) -> (Value, StatusCode) {
        self.service.get(format!("/indexes{parameters}")).await
    }

    pub async fn tasks_filter(&self, filter: &str) -> (Value, StatusCode) {
        self.service.get(format!("/tasks?{}", filter)).await
    }

    pub async fn batches_filter(&self, filter: &str) -> (Value, StatusCode) {
        self.service.get(format!("/batches?{}", filter)).await
    }

    pub async fn version(&self) -> (Value, StatusCode) {
        self.service.get("/version").await
    }

    pub async fn create_dump(&self) -> (Value, StatusCode) {
        self.service.post("/dumps", json!(null)).await
    }

    pub async fn create_snapshot(&self) -> (Value, StatusCode) {
        self.service.post("/snapshots", json!(null)).await
    }

    pub async fn index_swap(&self, value: Value) -> (Value, StatusCode) {
        self.service.post("/swap-indexes", value).await
    }

    pub async fn cancel_tasks(&self, value: &str) -> (Value, StatusCode) {
        self.service.post(format!("/tasks/cancel?{}", value), json!(null)).await
    }

    pub async fn delete_tasks(&self, value: &str) -> (Value, StatusCode) {
        self.service.delete(format!("/tasks?{}", value)).await
    }

    pub async fn wait_task(&self, update_id: u64) -> Value {
        // try several times to get status, or panic to not wait forever
        let url = format!("/tasks/{}", update_id);
        for _ in 0..100 {
            let (response, status_code) = self.service.get(&url).await;
            assert_eq!(200, status_code, "response: {}", response);

            if response["status"] == "succeeded" || response["status"] == "failed" {
                return response;
            }

            // wait 0.5 second.
            sleep(Duration::from_millis(500)).await;
        }
        panic!("Timeout waiting for update id");
    }

    pub async fn get_task(&self, update_id: u64) -> (Value, StatusCode) {
        let url = format!("/tasks/{}", update_id);
        self.service.get(url).await
    }

    pub async fn get_batch(&self, batch_id: u32) -> (Value, StatusCode) {
        let url = format!("/batches/{}", batch_id);
        self.service.get(url).await
    }

    pub async fn get_features(&self) -> (Value, StatusCode) {
        self.service.get("/experimental-features").await
    }
}

pub fn default_settings(dir: impl AsRef<Path>) -> Opt {
    Opt {
        db_path: dir.as_ref().join("db"),
        dump_dir: dir.as_ref().join("dumps"),
        env: "development".to_owned(),
        no_analytics: true,
        max_index_size: Byte::from_u64_with_unit(100, Unit::MiB).unwrap(),
        max_task_db_size: Byte::from_u64_with_unit(1, Unit::GiB).unwrap(),
        http_payload_size_limit: Byte::from_u64_with_unit(10, Unit::MiB).unwrap(),
        snapshot_dir: ".".into(),
        indexer_options: IndexerOpts {
            // memory has to be unlimited because several meilisearch are running in test context.
            max_indexing_memory: MaxMemory::unlimited(),
            skip_index_budget: true,
            // Having 2 threads makes the tests way faster
            max_indexing_threads: MaxThreads::from_str("2").unwrap(),
        },
        experimental_enable_metrics: false,
        ..Parser::parse_from(None as Option<&str>)
    }
}
