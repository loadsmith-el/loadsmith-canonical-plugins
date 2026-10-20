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
