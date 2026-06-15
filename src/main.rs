use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use rustls::crypto::ring;
use url::Url;

#[tokio::main]
async fn main() -> Result<()> {
    ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");
    let url = Url::parse("wss://fstream.binance.com/market/ws/btcusdt@aggTrade")?;

    let (ws_stream, _) = connect_async(url.as_str()).await.expect("Failed to connect");

    println!("WebSocket handshake has been successfully completed");

    let (mut write, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        let msg = msg?;

        if msg.is_text() {
            let text = msg.to_text()?;
            println!("{}", text);
        }
    }

    Ok(())
    
}
