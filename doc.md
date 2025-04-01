# ccxt-proxy API 文档

本 API 文档描述了 ccxt-proxy 提供的 RESTful 和 WebSocket 接口，用于与加密货币交易所进行交互。

## 配置

API 的配置信息存储在 `data/config.json` 文件中。您需要在此文件中设置 API 令牌、交易所 API 密钥和代理信息。

## 环境变量

* `API_TOKEN`: 用于验证 API 请求的令牌。

## RESTful API

### 1. Hello

* **路径:** `/hello`
* **方法:** GET
* **描述:** 测试 API 是否可用。
* **请求参数:**
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /hello?token=your_token`
* **响应示例:**

    ```json
    {
      "hello": "world"
    }
    ```

### 2. 获取 OHLCV 数据（起始时间 + 数量模式）

* **路径:** `/api/ohlcv`
* **方法:** GET
* **描述:** 根据起始时间和数量获取 OHLCV 数据。
* **请求参数:**
    * `symbol` (query): 交易对，例如 "BTC/USDT"。
    * `timeframe` (query): 时间周期，例如 "15m"。
    * `since` (query, 可选): 起始时间戳（毫秒）。
    * `limit` (query, 可选): 返回的数据点数量。
    * `cache` (query, 可选): 是否使用缓存，默认为 `true`。
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /api/ohlcv?symbol=BTC/USDT&timeframe=15m&since=1677657600000&limit=100&cache=true&token=your_token`
* **响应示例:**

    ```json
    {
      "symbol": "BTC/USDT",
      "ohlcv": [
        [1677657600000, 20000, 21000, 19000, 20500, 100],
        // ...
      ]
    }
    ```

### 3. 获取 OHLCV 数据（连续周期模式）

* **路径:** `/api/ohlcv_period`
* **方法:** GET
* **描述:** 根据起始日期和周期数量获取 OHLCV 数据。
* **请求参数:**
    * `symbol` (query): 交易对，例如 "BTC/USDT"。
    * `timeframe` (query): 时间周期，例如 "15m"。
    * `start_date` (query): 起始日期，格式为 "YYYY-MM-DD"。
    * `periods` (query): 连续周期数量（天数或月数）。
    * `cache` (query, 可选): 是否使用缓存，默认为 `true`。
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /api/ohlcv_period?symbol=BTC/USDT&timeframe=15m&start_date=2023-03-01&periods=3&cache=true&token=your_token`
* **响应示例:**

    ```json
    {
      "symbol": "BTC/USDT",
      "ohlcv": [
        [1677657600000, 20000, 21000, 19000, 20500, 100],
        // ...
      ]
    }
    ```

### 4. 获取最新价格（RESTful 备用接口）

* **路径:** `/api/ticker`
* **方法:** GET
* **描述:** 获取指定交易对的最新价格。
* **请求参数:**
    * `symbol` (query): 交易对，例如 "BTC/USDT"。
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /api/ticker?symbol=BTC/USDT&token=your_token`
* **响应示例:**

    ```json
    {
      "symbol": "BTC/USDT",
      "price": 21000
    }
    ```

### 5. 创建市价/限价单（支持止盈止损）

* **路径:** `/api/order`
* **方法:** POST
* **描述:** 创建市价或限价订单，支持设置止盈和止损。
* **请求参数:**
    * `token` (query): API 令牌。
    * **Body:**
        * `symbol` (string): 交易对，例如 "BTC/USDT"。
        * `side` (string): 订单方向，"buy" 或 "sell"。
        * `amount` (float): 订单数量。
        * `price` (float, 可选): 限价单价格。
        * `take_profit` (float, 可选): 止盈价格。
        * `stop_loss` (float, 可选): 止损价格。
* **调用示例:**
    * `POST /api/order?token=your_token`
    * Body: `{"symbol": "BTC/USDT", "side": "buy", "amount": 0.1, "price": 50000, "take_profit": 55000, "stop_loss": 45000}`
* **响应示例:**

    ```json
    {
      "id": "12345",
      "symbol": "BTC/USDT",
      "type": "limit",
      "side": "buy",
      "amount": 0.1,
      "price": 50000,
      "stop_orders": [
        // ...
      ]
    }
    ```

### 6. 创建市价止盈止损单（触发价格）

* **路径:** `/api/stop_order`
* **方法:** POST
* **描述:** 创建市价止盈止损订单，指定触发价格。
* **请求参数:**
    * `token` (query): API 令牌。
    * **Body:**
        * `symbol` (string): 交易对，例如 "BTC/USDT"。
        * `side` (string): 订单方向，"buy" 或 "sell"。
        * `amount` (float): 订单数量。
        * `trigger_price` (float): 触发价格。
* **调用示例:**
    * `POST /api/stop_order?token=your_token`
    * Body: `{"symbol": "BTC/USDT", "side": "buy", "amount": 0.1, "trigger_price": 50000}`
* **响应示例:**

    ```json
    {
      "id": "12346",
      "symbol": "BTC/USDT",
      "type": "stopMarket",
      "side": "buy",
      "amount": 0.1,
      "stopPrice": 50000
    }
    ```

### 7. 创建跟踪止损单

* **路径:** `/api/trailing_stop`
* **方法:** POST
* **描述:** 创建跟踪止损订单。
* **请求参数:**
    * `token` (query): API 令牌。
    * **Body:**
        * `symbol` (string): 交易对，例如 "BTC/USDT"。
        * `side` (string): 订单方向，"buy" 或 "sell"。
        * `amount` (float): 订单数量。
        * `distance` (float): 跟踪距离。
* **调用示例:**
    * `POST /api/trailing_stop?token=your_token`
    * Body: `{"symbol": "BTC/USDT", "side": "buy", "amount": 0.1, "distance": 1000}`
* **响应示例:**

    ```json
    {
      "id": "12347",
      "symbol": "BTC/USDT",
      "type": "trailingStop",
      "side": "buy",
      "amount": 0.1,
      "trailingDelta": 1000
    }
    ```

### 8. 取消订单

* **路径:** `/api/cancel_order`
* **方法:** POST
* **描述:** 取消指定订单。
* **请求参数:**
    * `order_id` (query): 订单 ID。
    * `symbol` (query): 交易对，例如 "BTC/USDT"。
    * `token` (query): API 令牌。
* **调用示例:**
    * `POST /api/cancel_order?order_id=12345&symbol=BTC/USDT&token=your_token`
* **响应示例:**

    ```json
    {
      "id": "12345",
      "status": "canceled"
    }
    ```

### 9. 查询订单

* **路径:** `/api/order`
* **方法:** GET
* **描述:** 查询指定订单的信息。
* **请求参数:**
    * `order_id` (query): 订单 ID。
    * `symbol` (query): 交易对，例如 "BTC/USDT"。
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /api/order?order_id=12345&symbol=BTC/USDT&token=your_token`
* **响应示例:**

    ```json
    {
      "id": "12345",
      "symbol": "BTC/USDT",
      "status": "closed"
    }
    ```

### 10. 查询当前持仓

* **路径:** `/api/positions`
* **方法:** GET
* **描述:** 查询当前持仓信息。
* **请求参数:**
    * `symbol` (query, 可选): 交易对，例如 "BTC/USDT"。
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /api/positions?symbol=BTC/USDT&token=your_token`
* **响应示例:**

    ```json
    [
      // ...
    ]
    ```

### 11. 查询历史持仓

* **路径:** `/api/history_positions`
* **方法:** GET
* **描述:** 查询历史持仓信息。
* **请求参数:**
    * `symbol` (query, 可选): 交易对，例如 "BTC/USDT"。
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /api/history_positions?symbol=BTC/USDT&token=your_token`
* **响应示例:**

    ```json
    [
      // ...
    ]
    ```

### 12. 查询当前委托

* **路径:** `/api/open_orders`
* **方法:** GET
* **描述:** 查询当前委托订单。
* **请求参数:**
    * `symbol` (query, 可选): 交易对，例如 "BTC/USDT"。
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /api/open_orders?symbol=BTC/USDT&token=your_token`
* **响应示例:**

    ```json
    [
      // ...
    ]
    ```

### 13. 查询历史委托

* **路径:** `/api/history_orders`
* **方法:** GET
* **描述:** 查询历史委托订单。
* **请求参数:**
    * `symbol` (query, 可选): 交易对，例如 "BTC/USDT"。
    * `since` (query, 可选): 起始时间戳（毫秒）。
    * `limit` (query, 可选): 返回的订单数量。
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /api/history_orders?symbol=BTC/USDT&since=1677657600000&limit=50&token=your_token`
* **响应示例:**

    ```json
    [
      // ...
    ]
    ```

### 14. 查询余额

* **路径:** `/api/balance`
* **方法:** GET
* **描述:** 查询账户余额。
* **请求参数:**
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /api/balance?token=your_token`
* **响应示例:**

    ```json
    {
      "USDT": {
        "free": 1000,
        "used": 500,
        "total": 1500
      },
      // ...
    }
    ```

### 15. 设置杠杆

* **路径:** `/api/leverage`
* **方法:** POST
* **描述:** 设置交易对的杠杆。
* **请求参数:**
    * `token` (query): API 令牌。
    * **Body:**
        * `symbol` (string): 交易对，例如 "BTC/USDT"。
        * `leverage` (int): 杠杆倍数。
* **调用示例:**
    * `POST /api/leverage?token=your_token`
    * Body: `{"symbol": "BTC/USDT", "leverage": 10}`
* **响应示例:**

    ```json
    {
      "symbol": "BTC/USDT",
      "leverage": 10
    }
    ```

### 16. 查询杠杆

* **路径:** `/api/leverage`
* **方法:** GET
* **描述:** 查询交易对的杠杆。
* **请求参数:**
    * `symbol` (query): 交易对，例如 "BTC/USDT"。
    * `token` (query): API 令牌。
* **调用示例:**
    * `GET /api/leverage?symbol=BTC/USDT&token=your_token`
* **响应示例:**

    ```json
    {
      "symbol": "BTC/USDT",
      "leverage": 10
    }
    ```

## WebSocket API

### 1. 订阅最新价格

* **路径:** `/ws/ticker`
* **描述:** 订阅指定交易对的最新价格。
* **请求参数:**
    * `symbol` (query): 交易对，例如 "BTC/USDT"。
    * `token` (query): API 令牌。
* **调用示例:**
    * `ws://localhost:8000/ws/ticker?symbol=BTC/USDT&token=your_token`
* **响应示例:**

    ```json
    {
      "symbol": "BTC/USDT",
      "price": 21000
    }
    ```
