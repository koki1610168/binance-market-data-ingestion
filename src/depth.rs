use anyhow::{Result, bail};
use serde::Deserialize;
use serde_json::{json};
use reqwest::Url;
use std::collections::BTreeMap;
use rust_decimal::Decimal;

// Need StreamExt to split the WebSocket
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio::sync::mpsc;
use rustls::crypto::ring;
use std::fs::OpenOptions;
use csv;

#[derive(Debug, Deserialize, Clone)]
struct Level(
    #[serde(with="rust_decimal::serde::str")]
    Decimal,

    #[serde(with="rust_decimal::serde::str")]
    Decimal,
);

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
    async fn get(symbol: &str) -> anyhow::Result<Self> {
        let url = format!("https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000");

        let url = Url::parse(&*url)?;
        let res = reqwest::get(url).await?;
        println!("status: {}", res.status());

        let text = res.text().await?;
        println!("raw response: {}", text);


        let snapshot: OrderBookSnapshot = serde_json::from_str(&text)?;


        Ok(snapshot)
    }
}

#[derive(Debug)]
struct LocalOrderBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,

    last_update_id: u64,
}

impl LocalOrderBook {
    fn from_snapshot(snapshot: OrderBookSnapshot) -> Self {
        let mut book = Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: snapshot.last_update_id
        };

        Self::apply_side(&mut book.bids, snapshot.bids);
        Self::apply_side(&mut book.asks, snapshot.asks);

        book
    }

    fn apply_first_diff(&mut self, diff: OrderBookDiff) {
        Self::apply_side(&mut self.bids, diff.bids);
        Self::apply_side(&mut self.asks, diff.asks);
        self.last_update_id = diff.final_id;
    }

    fn apply_next_diff(&mut self, diff: OrderBookDiff) -> Result<()> {
        if diff.prev_final_id != self.last_update_id {
            bail!("diff mismatch");
        }

        Self::apply_side(&mut self.bids, diff.bids);
        Self::apply_side(&mut self.asks, diff.asks);
        self.last_update_id = diff.final_id;

        Ok(())
    }

    fn apply_side(side: &mut BTreeMap<Decimal, Decimal>, levels: Vec<Level>) {
        for Level(price, qty) in levels {
            if qty == Decimal::ZERO {
                side.remove(&price);
            } else {
                side.insert(price, qty);
            }
        }

    }

    fn best_bid(&self) -> Option<(&Decimal, &Decimal)> {
        self.bids.iter().next_back()
    }

    fn best_ask(&self) -> Option<(&Decimal, &Decimal)> {
        self.asks.iter().next()
    }
}

async fn spawn_ws_reader(symbol_lower: &str) -> Result<mpsc::Receiver<OrderBookDiff>> {
    let url = format!("wss://fstream.binance.com/public/ws/{symbol_lower}@depth");

    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    println!("WebSocket handshake has been successfully completed");

    let (_, mut read) = ws_stream.split();

    let (tx, rx) = mpsc::channel::<OrderBookDiff>(50_000);

    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            match msg {
                Ok(Message::Text(text)) => {
                    match serde_json::from_str::<OrderBookDiff>(&text) {
                        Ok(diff) => {
                            if tx.send(diff).await.is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("failed to parse diff")
                        }
                    }
                }
                Ok(Message::Ping(_)) => {}
                Ok(Message::Pong(_)) => {}
                Ok(Message::Close(frame)) => {
                    eprintln!("WebSocket closed: {frame:?}");
                    break;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("WebSocket error: {e}");
                    break;
                }

            }

        }
    });

    Ok(rx)
    
}

pub async fn run_depth_collector() -> anyhow::Result<()> {
    // Somehow necessary to define defalt provider
    ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    
    let symbol = "btcusdt";
    let symbol_lower = symbol.to_lowercase();

    loop {
        println!("starting local order book initialization");

        let mut rx = spawn_ws_reader(&symbol_lower).await?;

        let snapshot = OrderBookSnapshot::get(symbol).await?;

        let last_update_id = snapshot.last_update_id;

        let mut book = LocalOrderBook::from_snapshot(snapshot);

        let mut buffer = Vec::new();

        loop {
            let diff = rx.recv().await.ok_or_else(|| anyhow::anyhow!("Websocket rx stopped"))?;

            if diff.final_id < last_update_id {
                continue;
            }

            buffer.push(diff);

            let first_pos = buffer.iter().position(|d| {
                d.first_id <= last_update_id && d.final_id >= last_update_id
            });

            if let Some(pos) = first_pos {
                let first = buffer.remove(pos);
                book.apply_first_diff(first);

                for diff in buffer.drain(pos..) {
                    book.apply_next_diff(diff)?;
                }

                break;

            }
        }
        println!("local order book initialzied");

        let mut need_resync = false;

        while let Some(diff) = rx.recv().await {
            if let Err(e) = book.apply_next_diff(diff) {
                eprintln!("{e}");
                need_resync = true;
                break;
            }

            if let (Some((bid_px, bid_qty)), Some((ask_px, ask_qty))) = 
                (book.best_bid(), book.best_ask()) {
                println!(
                    "bid={}, qty={} | ask={}, qty={} | last_update_id={}",
                    bid_px, bid_qty, ask_px, ask_qty, book.last_update_id
                );
            }
        }
        if need_resync {
            println!("resyncing from REST snapshot ...");
            continue;
        }
        
        bail!("websocket ended");
    }

}

