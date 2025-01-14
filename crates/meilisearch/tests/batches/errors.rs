use meili_snap::*;

use crate::common::Server;

#[actix_rt::test]
async fn batch_bad_uids() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("uids=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `uids`: could not parse `doggo` as a positive integer",
      "code": "invalid_task_uids",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_uids"
    }
    "#);
}

#[actix_rt::test]
async fn batch_bad_canceled_by() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("canceledBy=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `canceledBy`: could not parse `doggo` as a positive integer",
      "code": "invalid_task_canceled_by",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_canceled_by"
    }
    "#);
}

#[actix_rt::test]
async fn batch_bad_types() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("types=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `types`: `doggo` is not a valid task type. Available types are `documentAdditionOrUpdate`, `documentEdition`, `documentDeletion`, `settingsUpdate`, `indexCreation`, `indexDeletion`, `indexUpdate`, `indexSwap`, `taskCancelation`, `taskDeletion`, `dumpCreation`, `snapshotCreation`, `upgradeDatabase`.",
      "code": "invalid_task_types",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_types"
    }
    "#);
}

#[actix_rt::test]
async fn batch_bad_statuses() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("statuses=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `statuses`: `doggo` is not a valid task status. Available statuses are `enqueued`, `processing`, `succeeded`, `failed`, `canceled`.",
      "code": "invalid_task_statuses",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_statuses"
    }
    "#);
}

#[actix_rt::test]
async fn batch_bad_index_uids() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("indexUids=the%20good%20doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `indexUids`: `the good doggo` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn batch_bad_limit() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("limit=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `limit`: could not parse `doggo` as a positive integer",
      "code": "invalid_task_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_limit"
    }
    "#);
}

#[actix_rt::test]
async fn batch_bad_from() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("from=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `from`: could not parse `doggo` as a positive integer",
      "code": "invalid_task_from",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_from"
    }
    "#);
}

#[actix_rt::test]
async fn bask_bad_reverse() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("reverse=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value in parameter `reverse`: could not parse `doggo` as a boolean, expected either `true` or `false`",
      "code": "invalid_task_reverse",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_reverse"
    }
    "###);

    let (response, code) = server.batches_filter("reverse=*").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value in parameter `reverse`: could not parse `*` as a boolean, expected either `true` or `false`",
      "code": "invalid_task_reverse",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_reverse"
    }
    "###);
}

#[actix_rt::test]
async fn batch_bad_after_enqueued_at() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("afterEnqueuedAt=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `afterEnqueuedAt`: `doggo` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_enqueued_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_after_enqueued_at"
    }
    "#);
}

#[actix_rt::test]
async fn batch_bad_before_enqueued_at() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("beforeEnqueuedAt=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `beforeEnqueuedAt`: `doggo` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_enqueued_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_before_enqueued_at"
    }
    "#);
}

#[actix_rt::test]
async fn batch_bad_after_started_at() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("afterStartedAt=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `afterStartedAt`: `doggo` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_started_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_after_started_at"
    }
    "#);
}

#[actix_rt::test]
async fn batch_bad_before_started_at() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("beforeStartedAt=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `beforeStartedAt`: `doggo` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_started_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_before_started_at"
    }
    "#);
}

#[actix_rt::test]
async fn batch_bad_after_finished_at() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("afterFinishedAt=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `afterFinishedAt`: `doggo` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_after_finished_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_after_finished_at"
    }
    "#);
}

#[actix_rt::test]
async fn batch_bad_before_finished_at() {
    let server = Server::new_shared();

    let (response, code) = server.batches_filter("beforeFinishedAt=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value in parameter `beforeFinishedAt`: `doggo` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.",
      "code": "invalid_task_before_finished_at",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_task_before_finished_at"
    }
    "#);
}
