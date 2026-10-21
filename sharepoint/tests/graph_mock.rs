//! End-to-end tests of the SharePoint source against a *fake* Graph (httpmock) —
//! no real tenant required. Exercises the full `configure()` path: OAuth token →
//! fetch (file download / list paging) → parse → transforms → Arrow.

use httpmock::prelude::*;
use loadsmith_plugin_sdk::SourcePlugin;
use loadsmith_sharepoint::SharepointSourcePlugin;

#[tokio::test]
async fn file_mode_csv_end_to_end() {
    let server = MockServer::start_async().await;

    server
        .mock_async(|when, then| {
            when.method(POST).path("/tenant-1/oauth2/v2.0/token");
            then.status(200)
                .json_body(serde_json::json!({ "access_token": "tok", "expires_in": 3600 }));
        })
        .await;

    server
        .mock_async(|when, then| {
            when.method(GET).path_contains("/content");
            then.status(200).body("id,area\n1,CIVEL\n2,TRAB\n");
        })
        .await;

    let cfg = serde_json::json!({
        "tenant_id": "tenant-1", "client_id": "c", "client_secret": "s",
        "site_id": "site-1", "drive_id": "drive-1",
        "graph_base_url": server.base_url(),
        "authority_url": server.base_url(),
        "input": { "kind": "file", "file": "/data.csv", "parser": { "kind": "csv" } },
        "transform": [
            { "kind": "filter", "where": { "column": "area", "op": "eq", "value": "CIVEL" } }
        ]
    });

    let mut plugin = SharepointSourcePlugin::new();
    plugin.configure(loadsmith_plugin_sdk::PluginConfig::new(cfg, loadsmith_plugin_sdk::UnknownKeys::Error)).await.expect("configure failed");

    let schema = plugin.schema().await.unwrap();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");

    let batch = plugin.next_batch().await.unwrap().expect("a batch");
    assert_eq!(batch.num_rows(), 1); // only the CIVEL row survives the filter
    assert!(plugin.next_batch().await.unwrap().is_none());
}

#[tokio::test]
async fn list_mode_paginates() {
    let server = MockServer::start_async().await;

    server
        .mock_async(|when, then| {
            when.method(POST).path("/tenant-1/oauth2/v2.0/token");
            then.status(200)
                .json_body(serde_json::json!({ "access_token": "tok", "expires_in": 3600 }));
        })
        .await;

    // List resolution returns no match → the plugin treats "list-1" as an id.
    server
        .mock_async(|when, then| {
            when.method(GET).path("/v1.0/sites/site-1/lists");
            then.status(200).json_body(serde_json::json!({ "value": [] }));
        })
        .await;

    let next = format!("{}/page2", server.base_url());
    server
        .mock_async(move |when, then| {
            when.method(GET).path("/v1.0/sites/site-1/lists/list-1/items");
            then.status(200).json_body(serde_json::json!({
                "value": [ { "fields": { "Title": "a", "Qtd": 1 } } ],
                "@odata.nextLink": next
            }));
        })
        .await;

    server
        .mock_async(|when, then| {
            when.method(GET).path("/page2");
            then.status(200).json_body(serde_json::json!({
                "value": [ { "fields": { "Title": "b", "Qtd": 2 } } ]
            }));
        })
        .await;

    let cfg = serde_json::json!({
        "tenant_id": "tenant-1", "client_id": "c", "client_secret": "s",
        "site_id": "site-1",
        "graph_base_url": server.base_url(),
        "authority_url": server.base_url(),
        "input": { "kind": "list", "list": "list-1" }
    });

    let mut plugin = SharepointSourcePlugin::new();
    plugin.configure(loadsmith_plugin_sdk::PluginConfig::new(cfg, loadsmith_plugin_sdk::UnknownKeys::Error)).await.expect("configure failed");

    let schema = plugin.schema().await.unwrap();
    assert_eq!(schema.field(0).name(), "Title");

    let batch = plugin.next_batch().await.unwrap().expect("a batch");
    assert_eq!(batch.num_rows(), 2); // both pages collected
}

fn plugin_config(v: serde_json::Value) -> loadsmith_plugin_sdk::PluginConfig {
    loadsmith_plugin_sdk::PluginConfig::new(v, loadsmith_plugin_sdk::UnknownKeys::Error)
}

#[tokio::test]
async fn throttling_is_retried_then_bounded() {
    let server = MockServer::start_async().await;
    let scope = format!("{}/.default", server.base_url());
    // The token scope follows `graph_base_url` (here the mock's origin).
    let token = server
        .mock_async(|when, then| {
            when.method(POST).path("/tenant-1/oauth2/v2.0/token").x_www_form_urlencoded_tuple("scope", &scope);
            then.status(200).json_body(serde_json::json!({ "access_token": "tok", "expires_in": 3600 }));
        })
        .await;
    let busy = server
        .mock_async(|when, then| {
            when.method(GET).path_contains("/content");
            then.status(503).header("Retry-After", "0").body("busy");
        })
        .await;

    let cfg = serde_json::json!({
        "tenant_id": "tenant-1", "client_id": "c", "client_secret": "s",
        "site_id": "site-1", "drive_id": "drive-1",
        "graph_base_url": server.base_url(), "authority_url": server.base_url(),
        "max_retries": 2, "request_timeout": "5s",
        "input": { "kind": "file", "file": "/data.csv", "parser": { "kind": "csv" } }
    });
    let err = SharepointSourcePlugin::new().configure(plugin_config(cfg)).await.unwrap_err();
    assert!(format!("{err:#}").contains("503"), "{err:#}");
    token.assert_hits_async(1).await;
    busy.assert_hits_async(3).await; // the first try + 2 retries
}

#[tokio::test]
async fn config_is_strict_before_any_request() {
    let base = serde_json::json!({
        "tenant_id": "t", "client_id": "c", "client_secret": "s", "site_id": "s", "drive_id": "d",
        "graph_base_url": "http://127.0.0.1:9", "authority_url": "http://127.0.0.1:9",
        "input": { "kind": "file", "file": "/a.csv", "parser": { "kind": "csv" } }
    });
    let err = |patch: serde_json::Value| {
        let mut cfg = base.clone();
        for (k, v) in patch.as_object().unwrap() {
            cfg[k] = v.clone();
        }
        async move { format!("{:#}", SharepointSourcePlugin::new().configure(plugin_config(cfg)).await.unwrap_err()) }
    };
    let e = err(serde_json::json!({ "input": { "kind": "file", "file": "/a.csv", "parser": { "kind": "csv", "delimitr": ";" } } })).await;
    assert!(e.contains("`input.parser.delimitr`"), "{e}");
    let e = err(serde_json::json!({ "transform": [{ "kind": "drop_empty_rows" }] })).await;
    assert!(e.contains("did you mean `drop-empty-rows`?"), "{e}");
    let e = err(serde_json::json!({ "transform": [{ "kind": "normalize", "fillr": "_" }] })).await;
    assert!(e.contains("`transform[0].fillr`"), "{e}");
    let e = err(serde_json::json!({ "transform": [{ "kind": "filter", "where": { "column": "a", "op": "eq", "valu": 1 } }] })).await;
    assert!(e.contains("`transform[0].where.valu`"), "{e}");
    let e = err(serde_json::json!({ "request_timeout": "30" })).await;
    assert!(e.contains("request_timeout"), "{e}");
    let e = err(serde_json::json!({ "scope": " " })).await;
    assert!(e.contains("`scope` is empty"), "{e}");
}
