use anyhow::Result;
use serde::Deserialize;
use serde_json::{json};

// Need StreamExt to split the WebSocket
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use rustls::crypto::ring;
use std::fs::OpenOptions;
use csv;

#[derive(Debug, Deserialize)]
struct Depth {
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
    bids: Vec<[String; 2]>,

    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}


pub async fn run_depth_collector() -> Result<()> {
    // Somehow necessary to define defalt provider
    ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    
    let url = "wss://fstream.binance.com/public/ws/btcusdt@depth";

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    println!("WebSocket handshake has been successfully completed");

    let (mut write, mut read) = ws_stream.split();

    let mut prev_final_id: u64 = 0;
    while let Some(msg) = read.next().await {
        match msg {
            Ok(msg) => {
                if let Ok(text) = msg.to_text() {
                    let depth: Depth = serde_json::from_str(text)?;

                    if prev_final_id != 0 && prev_final_id != depth.prev_final_id {
                        eprintln!("Depth lost");
                        break;
                    } else {
                        println!("{:#?}", depth);
                        prev_final_id = depth.final_id;
                    }
                }
            }
            Err(e) => {
                eprintln!("WebSocket error: {e}");
                break;
            }
        }
    }


    Ok(())
    
}

