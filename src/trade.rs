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


pub async fn run_trade_collector() -> Result<()> {
    // Somehow necessary to define defalt provider
    ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    
    let url = "wss://fstream.binance.com/market/ws/btcusdt@aggTrade";

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    println!("WebSocket handshake has been successfully completed");

    let (mut write, mut read) = ws_stream.split();

    // csv file creation
    let csv_file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open("./data/trades/BTC/btc_trade.csv")?;

    let mut csv_writer = csv::Writer::from_writer(csv_file);
    csv_writer.write_record(&["event_time", "symbol", "price", "qty", "is_mm"]);

    let mut count: u32 = 1;
    while let Some(msg) = read.next().await {
        match msg {
            Ok(msg) => {
                if let Ok(text) = msg.to_text() {
                    let trade: AggrTrade = serde_json::from_str(text)?;

                    csv_writer.serialize((trade.event_time, &trade.symbol, &trade.price, &trade.qty, trade.is_mm))?;

                    println!("{} {} @ {}", &trade.symbol, &trade.qty, &trade.price);
                    
                    if count == 100 {
                        csv_writer.flush()?;
                        break;
                    }
                    count += 1;


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
