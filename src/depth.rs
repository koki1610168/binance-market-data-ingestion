use anyhow::Result;
use serde::Deserialize;
use serde_json::{json};
use reqwest::Url;

// Need StreamExt to split the WebSocket
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use rustls::crypto::ring;
use std::fs::OpenOptions;
use csv;

#[derive(Debug, Deserialize)]
struct Level(String, String);


#[derive(Debug, Deserialize)]
struct OrderBookDiff {
    #[serde(rename = "e")]
    event_type: String,

    #[serde(rename = "E")]
    event_time: u64,

    #[serde(rename = "T")]
    tx_time: u64,

    #[serde(rename = "s")]
    symbol: String,

    #[serde(rename = "U")]
    first_id: u64,

    #[serde(rename = "u")]
    final_id: u64,

    #[serde(rename = "pu")]
    prev_final_id: u64,

    #[serde(rename = "b")]
    bids: Vec<Level>,

    #[serde(rename = "a")]
    asks: Vec<Level>,
}


#[derive(Debug, Deserialize)]
struct OrderBookSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,


    #[serde(rename = "E")]
    output_time: u64,

    #[serde(rename = "T")]
    tx_time: u64,

    bids: Vec<Level>,

    asks: Vec<Level>,
}

impl OrderBookSnapshot {
    async fn get() -> anyhow::Result<Self> {
        let url = "https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000";

        let url = Url::parse(&*url)?;
        let res = reqwest::get(url).await?;
        println!("status: {}", res.status());

        let text = res.text().await?;
        println!("raw response: {}", text);


        let snapshot: OrderBookSnapshot = serde_json::from_str(&text)?;


        Ok(snapshot)

    }
}
pub async fn run_depth_collector() -> anyhow::Result<()> {
    // Somehow necessary to define defalt provider
    ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    
    // order book diff WebSocket URL
    let url = "wss://fstream.binance.com/public/ws/btcusdt@depth";

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    println!("WebSocket handshake has been successfully completed");

    let (mut write, mut read) = ws_stream.split();

    let mut prev_final_id: u64 = 0;

    let mut buffer_diff: Vec<OrderBookDiff> = Vec::new();
    while let Some(msg) = read.next().await {
        match msg {
            Ok(msg) => {
                if let Ok(text) = msg.to_text() {
                    let depth: OrderBookDiff = serde_json::from_str(text)?;

                    if prev_final_id != 0 && prev_final_id != depth.prev_final_id {
                        eprintln!("Depth lost");
                        break;
                    } else {
                        println!("{:#?}", depth);
                        prev_final_id = depth.final_id;
                        buffer_diff.push(depth);
                    }

                    if buffer_diff.len() >= 10 {
                        break;
                    }
                }
            }
            Err(e) => {
                eprintln!("WebSocket error: {e}");
                break;
            }
        }

    }

    let snapshot = OrderBookSnapshot::get().await?;
    println!("{:#?}", snapshot);
    println!("Done");

    Ok(())
}

