# Market Data Ingestion

## Overview
* Listening to WebSocket endpoin on Binance and maintain trades and orderbook data

## TODO
* [x] Listen to Binance WebSocket
    * [ ] Reconnection Logic
    * [ ] Heartbeat
* [X] Receive trades data
    * [X] Ensure data consistency
* [ ] Receive orderbook snapshot and delta
    * [ ] Ensure data consistency
    * [ ] How do we handle disconnection? -> get orderbook snapshot again
* [ ] Orderbook reconstruction code
* [ ] Design the Postgress Database
* [ ] Save the data in the Database

## Managing local Order Book
* [Binance official doc](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly)
    * Basically, connect to order book diff and buffer it. Then, get order book snapshot and process the buffered diff whose update time is later than the snapshot

