## When buying:

If RSI (closed) < 20 
and median 24h volume > @threshold
and total_coin_balance_usd < 5
and available_balance > 5, then: 

 0. Check if there is a pending order. If yes, cancel it.
 1. Get best bid price
 2. Get the current price (closedPrice in kline)
 3. Get available balance 
 4. If best bid >= current price, then place an order such that: 
 ⁃ limit price = best bid price 
 ⁃ order amount = available_balance - 5 
 ⁃ Stop limit = 0.95 * limit price
 5. If best bid < closed price, then place an order such that: 
 ⁃ Limit price = (best bid + current price)/2
 ⁃ Round DOWN limit price to the same precision as best bid
 ⁃ i.e. if best bid has 3 decimal points, the limit price should also be rounded to 3 decimals
 ⁃ Order amount = available_balance - 5
 6. Check if the order is filled:
 ⁃ If yes, done
 ⁃ If no, check RSI (live) every x seconds. 
          - If RSI (live) is below 20, cancel the pending order and open a new order with new set of buy parameters. 
          - If RSI (live) is above 20, terminate the process. 


## When selling: 

If RSI > 40 
And coin_balance_usd > 5 (true unless stop loss was hit already), then: 

 1. Get best ask price
 2. Get the current price (closedPrice in kline)
 3. Get coin balance that we are currently holding
 4. Cancel the order created by stop loss
 5. If best ask <= current price, then place an order such that: 
 ⁃ limit price = best ask
 ⁃ order amount = coin_balance
 6. If best bid < closed price, then place an order such that: 
 ⁃ Limit price = (best ask + current price)/2
 ⁃ Round UP limit price to the same precision as best bid
 ⁃ Order amount = coin_balance
 7. Check if the order is filled every x seconds.
        - If yes, done
        - If no, cancel the pending order and open a new order with new set of sell parameters. 
 8. If again not filled within x minutes (30 mins let say), place a market order with order size = coin_balance
