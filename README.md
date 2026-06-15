# Market Data Ingestion

## Overview
* Listening to WebSocket endpoin on Binance and maintain trades and orderbook data

## TODO
* [x] Listen to Binance WebSocket
    * [ ] Reconnection Logic
    * [ ] Heartbeat
* [ ] Receive trades data
    * [ ] Ensure data consistency
* [ ] Receive orderbook snapshot and delta
    * [ ] Ensure data consistency
    * [ ] How do we handle disconnection? -> get orderbook snapshot again
* [ ] Orderbook reconstruction code
* [ ] Design the Postgress Database
* [ ] Save the data in the Database

