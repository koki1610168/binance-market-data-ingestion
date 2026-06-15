use anyhow::Result;
use serde::Deserialize;
use serde_json::{json};

// Need StreamExt to split the WebSocket
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use rustls::crypto::ring;
use url::Url;

#[derive(Debug, Deserialize)]
struct AggrTrade {
    #[serde(rename = "e")]
    event_type: String,

    #[serde(rename = "E")]
    event_time: u64,

    #[serde(rename = "s")]
    symbol: String,

    #[serde(rename = "a")]
    aggr_trade_id: u64,

    #[serde(rename = "p")]
    price: String,

    #[serde(rename = "q")]
    qty: String,

    // quantity without RPI orders
    #[serde(rename = "nq")]
    normal_qty: String,

    #[serde(rename = "f")]
    first_trade_id: u64,

    #[serde(rename = "l")]
    last_trade_id: u64,

    #[serde(rename = "T")]
    trade_time: u64,

    #[serde(rename = "m")]
    is_mm: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Somehow necessary to define defalt provider
    ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    
    let url = "wss://fstream.binance.com/market/ws/btcusdt@aggTrade";

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    println!("WebSocket handshake has been successfully completed");

    let (mut write, mut read) = ws_stream.split();


    while let Some(msg) = read.next().await {
        match msg {
            Ok(msg) => {
                if let Ok(text) = msg.to_text() {
                    let trade: AggrTrade = serde_json::from_str(text)?;

                    let symbol: String = trade.symbol;
                    let price: f64 = trade.price.parse()?;
                    let qty: f64 = trade.qty.parse()?;
                    println!("{} {} @ {}", symbol, qty, price);
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
