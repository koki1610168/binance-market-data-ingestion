use tokio;
use anyhow::Result;

mod trade;
mod depth;

#[tokio::main]
async fn main() -> Result<()> {
    //let trade_task = tokio::spawn(trade::run_trade_collector());
    let depth_task = tokio::spawn(depth::run_depth_collector());

    let _ = tokio::join!(depth_task);

    Ok(())
    
}
