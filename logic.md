## When Buying:

- **Conditions**:
  - RSI (closed) < 20
  - Median 24h volume > threshold
  - Total coin balance in USD < 5
  - Available balance > 5

- **Procedure**:
  
  1. Check if there is a pending order. If yes, cancel it.
  2. Get best bid price.
  3. Get the current price (closedPrice in kline).
  4. Get available balance.
  5. If best bid >= current price, then place an order such that:
     - Limit price = best bid price.
     - Order amount = available balance - 5.
     - Stop limit = 0.95 * limit price.
  6. If best bid < closed price, then place an order such that:
     - Limit price = (best bid + current price)/2.
     - Round DOWN limit price to the same precision as best bid.
       - e.g., if best bid has 3 decimal points, the limit price should also be rounded to 3 decimals.
     - Order amount = available balance - 5.
  7. Check if the order is fully filled:
     - If yes, done.
     - If no, check RSI (live) every x seconds.
       - If RSI (live) is below 20, update the pending order with new set of buy parameters. 
       - If RSI (live) is above 20, terminate the process.

## When Selling:

- **Conditions**:
  - RSI > 40
  - Coin balance in USD > 5 (true unless stop loss was hit already).

- **Procedure**:
  1. Get best ask price.
  2. Get the current price (closedPrice in kline).
  3. Get coin balance that we are currently holding.
  4. Cancel the order created by stop loss.
  5. If best ask <= current price, then place an order such that:
     - Limit price = best ask.
     - Order amount = coin balance.
  6. If best bid < closed price, then place an order such that:
     - Limit price = (best ask + current price)/2.
     - Round UP limit price to the same precision as best bid.
     - Order amount = coin balance.
  7. Check if the order is fully filled every x seconds.
     - If yes, done.
     - If no, update the pending order with new set of sell parameters. 
  8. If again not filled within x minutes (30 mins, let's say), place a market order with order size = coin balance.
