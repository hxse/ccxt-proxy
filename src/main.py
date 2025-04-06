import json
import os
import asyncio
import ccxt.async_support as ccxt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional, Union, Dict
from datetime import datetime, timezone
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from cache.cache_manager import (
    handle_ohlcv_cache,
    get_ohlcv_data_from_exchange,
    TIMEFRAME_SECONDS,
    str2datetime,
)
import sys
import traceback

app = FastAPI()

# 加载 config.json
CONFIG_PATH = "data/config.json"
if not os.path.exists(CONFIG_PATH):
    raise ValueError(
        f"Config file {CONFIG_PATH} not found. Please create it with required tokens."
    )

with open(CONFIG_PATH, "r") as f:
    config = json.load(f)

API_TOKEN = config.get("api_token")
if not API_TOKEN:
    raise ValueError("API_TOKEN not set in config.json")

# CORS 配置
origins = ["*", "http://localhost"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# CCXT 初始化
mode = "test" if len(sys.argv) > 1 and sys.argv[1] == "test" else "live"
http_proxy = config["proxy"].get("http", None)
exchange = ccxt.binance(
    {
        "apiKey": config["binance"][mode].get("api_key"),
        "secret": config["binance"][mode].get("secret"),
        "enableRateLimit": True,
        "options": {"defaultType": config["type"]},
    }
)
exchange.httpProxy = http_proxy
if mode == "test":
    exchange.set_sandbox_mode(True)


# 验证 Token
def verify_token(token: str):
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token


async def handle_exceptions():
    try:
        yield
    except Exception as e:
        error_detail = "".join(traceback.format_exception(type(e), e, e.__traceback__))
        raise HTTPException(status_code=500, detail=error_detail)


# 请求模型
class OHLCVRequest(BaseModel):
    symbol: str
    timeframe: str
    since: Optional[Union[str, int]] = None  # 支持 ISO 或整数时间戳
    limit: int = 100
    cache: bool = True


class OrderRequest(BaseModel):
    symbol: str
    side: str
    amount: float
    price: Optional[float] = None
    take_profit: Optional[float] = None
    stop_loss: Optional[float] = None


class StopOrderRequest(BaseModel):
    symbol: str
    side: str
    amount: float
    trigger_price: float


class TrailingStopRequest(BaseModel):
    symbol: str
    side: str
    amount: float
    distance: float


class LeverageRequest(BaseModel):
    symbol: str
    leverage: int


# 转换 since 参数
def parse_since(since: Optional[Union[str, int]]) -> Optional[int]:
    if since is None or since == "":
        return None

    if isinstance(since, int):
        return since

    if isinstance(since, str):
        # 尝试将字符串转换为整数时间戳
        if since.isdigit():
            return int(since)

        # 尝试解析指定的格式 "YYYY-MM-DD HH:MM:SS" 并假设为 UTC
        try:
            dt = str2datetime(since)
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass  # 如果不匹配，继续尝试其他格式

        # 尝试解析 ISO 8601 格式
        formats = [
            lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")),  # 标准 ISO
            lambda s: datetime.fromisoformat(
                s.replace(" ", "T") + "+00:00"
            ),  # 非标准 ISO
        ]

        for format_func in formats:
            try:
                dt = format_func(since)
                return int(dt.timestamp() * 1000)
            except ValueError:
                pass  # 尝试下一个格式

        # 如果所有解析都失败
        raise HTTPException(
            status_code=400,
            detail="Invalid since format. Use ISO UTC (e.g., '2023-03-01T00:00:00Z' or '2023-03-01 00:00:00') or integer timestamp.",
        )

    # 如果输入类型无效
    raise HTTPException(
        status_code=400,
        detail="Invalid since format. Use ISO UTC or integer timestamp.",
    )


# RESTful API 路由
@app.get("/hello")
async def get_hello(token: str = Depends(verify_token)):
    """
    - GET /hello?token=your_token
    一个简单的测试路由，返回 "hello: world"。
    需要提供有效的 API token 进行身份验证。
    """
    return {"hello": "world"}


@app.get("/api/ohlcv")
async def get_ohlcv(
    request: OHLCVRequest = Depends(),
    token: str = Depends(lambda: "your_token"),  # Placeholder for verify_token
    exception_handler: None = Depends(
        lambda: None
    ),  # Placeholder for handle_exceptions
):
    """
    - GET /api/ohlcv?symbol=BTC/USDT&timeframe=15m&since=2023-01-01T00:00:00Z&limit=100&cache=true&token=your_token
    返回OHLCV数据 {"done_data": { "timestamp": [], "open": [], "high": [], "low": [], "close": [], "volume": [], "date": [] }, "last_data": { "timestamp": [], "open": [], "high": [], "low": [], "close": [], "volume": [], "date": [] }}
    since支持 ISO UTC时间戳, 整数时间戳
    2023-01-01T00:00:00Z, 2023-01-01 00:00:00, 1677657600000
    如果since为None, "", 则根据请求数量自动计算since

    缓存文件只保存已完成的k线,不保存未完成的k线
    cache=true, 直接从缓存获取已完成的K线和最新的未完成K线。
    cache=false, 关闭缓存, 从交易所获取已完成的K线和最新的未完成K线。
    """
    if request.timeframe not in TIMEFRAME_SECONDS:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")

    since = parse_since(request.since)
    if request.cache:
        result = await handle_ohlcv_cache(
            mode,
            exchange,
            request.symbol,
            request.timeframe,
            since,
            request.limit,
        )
    else:
        result = await get_ohlcv_data_from_exchange(
            exchange,
            request.symbol,
            request.timeframe,
            since,
            request.limit,
        )
    print([[i, len(result[i])] for i in result.keys()])
    result = {
        "done_data": result["done_data"].to_dict("list"),
        "last_data": result["last_data"].to_dict("list"),
    }
    return result


@app.get("/api/ticker")
async def get_ticker(
    symbol: str,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - GET /api/ticker?symbol=BTC/USDT&token=your_token
    获取指定交易对的最新 ticker 信息（仅返回最新价格）。
    需要提供有效的 API token 进行身份验证。

    参数:
    - symbol: 交易对，例如 "BTC/USDT"。
    """
    ticker = await exchange.fetch_ticker(symbol)
    return {"symbol": symbol, "price": ticker["last"]}


@app.post("/api/order")
async def create_order(
    request: OrderRequest,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - POST /api/order?token=your_token
    根据提供的参数创建一个新的订单。
    支持市价单和限价单，并可同时设置止盈和止损订单。
    需要提供有效的 API token 进行身份验证。

    请求体 (JSON):
    - symbol: 交易对，例如 "BTC/USDT"。
    - side: 订单方向，"buy" 或 "sell"。
    - amount: 订单数量。
    - price (可选): 限价单的价格。如果为 None，则创建市价单。
    - take_profit (可选): 止盈价格。
    - stop_loss (可选): 止损价格。
    """
    order_type = "market" if request.price is None else "limit"
    order = await exchange.create_order(
        request.symbol, order_type, request.side, request.amount, request.price
    )
    if request.take_profit or request.stop_loss:
        stop_orders = []
        if request.take_profit:
            stop_orders.append(
                await exchange.create_order(
                    request.symbol,
                    "takeProfit",
                    "sell" if request.side == "buy" else "buy",
                    request.amount,
                    request.take_profit,
                )
            )
        if request.stop_loss:
            stop_orders.append(
                await exchange.create_order(
                    request.symbol,
                    "stopLoss",
                    "sell" if request.side == "buy" else "buy",
                    request.amount,
                    request.stop_loss,
                )
            )
        order["stop_orders"] = stop_orders
    return order


@app.post("/api/stop_order")
async def create_stop_order(
    request: StopOrderRequest,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - POST /api/stop_order?token=your_token
    创建一个止损订单。当市场价格达到触发价格时，会创建一个市价单。
    需要提供有效的 API token 进行身份验证。

    请求体 (JSON):
    - symbol: 交易对，例如 "BTC/USDT"。
    - side: 订单方向，"buy" 或 "sell"。
    - amount: 订单数量。
    - trigger_price: 触发价格。当市场价格达到此价格时触发。
    """
    order = await exchange.create_order(
        request.symbol,
        "stopMarket",
        request.side,
        request.amount,
        None,
        {"stopPrice": request.trigger_price},
    )
    return order


@app.post("/api/trailing_stop")
async def create_trailing_stop(
    request: TrailingStopRequest,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - POST /api/trailing_stop?token=your_token
    创建一个追踪止损订单。止损价格会跟随市场价格波动。
    需要提供有效的 API token 进行身份验证。

    请求体 (JSON):
    - symbol: 交易对，例如 "BTC/USDT"。
    - side: 订单方向，"buy" 或 "sell"。
    - amount: 订单数量。
    - distance: 追踪止损的距离。
    """
    order = await exchange.create_order(
        request.symbol,
        "trailingStop",
        request.side,
        request.amount,
        None,
        {"trailingDelta": request.distance},
    )
    return order


@app.post("/api/cancel_order")
async def cancel_order(
    order_id: str,
    symbol: str,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - POST /api/cancel_order?order_id=YOUR_ORDER_ID&symbol=BTC/USDT&token=your_token
    取消指定 ID 的订单。
    需要提供有效的 API token 进行身份验证。

    参数:
    - order_id (query): 要取消的订单 ID。
    - symbol (query): 订单所属的交易对，例如 "BTC/USDT"。
    """
    result = await exchange.cancel_order(order_id, symbol)
    return result


@app.get("/api/order")
async def fetch_order(
    order_id: str,
    symbol: str,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - GET /api/order?order_id=YOUR_ORDER_ID&symbol=BTC/USDT&token=your_token
    获取指定 ID 的订单信息。
    需要提供有效的 API token 进行身份验证。

    参数:
    - order_id (query): 要查询的订单 ID。
    - symbol (query): 订单所属的交易对，例如 "BTC/USDT"。
    """
    order = await exchange.fetch_order(order_id, symbol)
    return order


@app.get("/api/positions")
async def fetch_positions(
    symbol: Optional[str] = None,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - GET /api/positions?symbol=BTC/USDT&token=your_token (可选 symbol)
    获取当前持仓信息。可以指定交易对进行查询。
    需要提供有效的 API token 进行身份验证。

    参数:
    - symbol (query, 可选): 要查询的交易对，例如 "BTC/USDT"。如果省略，则返回所有持仓。
    """
    positions = await exchange.fetch_positions(symbol)
    return positions


@app.get("/api/history_positions")
async def fetch_history_positions(
    symbol: Optional[str] = None,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - GET /api/history_positions?symbol=BTC/USDT&token=your_token (可选 symbol)
    获取历史持仓信息（已平仓的持仓）。可以指定交易对进行查询。
    需要提供有效的 API token 进行身份验证。

    参数:
    - symbol (query, 可选): 要查询的交易对，例如 "BTC/USDT"。如果省略，则返回所有历史持仓。
    """
    positions = await exchange.fetch_closed_positions(symbol)
    return positions


@app.get("/api/open_orders")
async def fetch_open_orders(
    symbol: Optional[str] = None,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - GET /api/open_orders?symbol=BTC/USDT&token=your_token (可选 symbol)
    获取当前未成交的订单信息。可以指定交易对进行查询。
    需要提供有效的 API token 进行身份验证。

    参数:
    - symbol (query, 可选): 要查询的交易对，例如 "BTC/USDT"。如果省略，则返回所有未成交订单。
    """
    orders = await exchange.fetch_open_orders(symbol)
    return orders


@app.get("/api/history_orders")
async def fetch_history_orders(
    symbol: Optional[str] = None,
    since: Optional[int] = None,
    limit: Optional[int] = 100,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - GET /api/history_orders?symbol=BTC/USDT&since=1672531200000&limit=100&token=your_token (可选 since 和 limit)
    获取历史订单信息（已成交或已取消的订单）。可以指定交易对、起始时间和数量进行查询。
    需要提供有效的 API token 进行身份验证。

    参数:
    - symbol (query, 可选): 要查询的交易对，例如 "BTC/USDT"。如果省略，则返回所有历史订单。
    - since (query, 可选): 起始时间戳（毫秒）。
    - limit (query, 可选): 返回的订单数量限制。
    """
    orders = await exchange.fetch_closed_orders(symbol, since, limit)
    return orders


@app.get("/api/balance")
async def fetch_balance(
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - GET /api/balance?token=your_token
    获取账户余额信息。
    需要提供有效的 API token 进行身份验证。
    """
    balance = await exchange.fetch_balance()
    return balance


@app.get("/api/leverage")
async def get_leverage(
    symbol: str,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - GET /api/leverage?symbol=BTC/USDT&token=your_token
    获取指定交易对的当前杠杆倍数。
    需要提供有效的 API token 进行身份验证。

    参数:
    - symbol (query): 交易对，例如 "BTC/USDT"。
    """
    leverage = await exchange.fetch_leverage(symbol)
    return {"symbol": symbol, "leverage": leverage}


@app.post("/api/leverage")
async def set_leverage(
    request: LeverageRequest,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """
    - POST /api/leverage?token=your_token
    设置指定交易对的杠杆倍数。
    需要提供有效的 API token 进行身份验证。

    请求体 (JSON):
    - symbol: 交易对，例如 "BTC/USDT"。
    - leverage: 要设置的杠杆倍数。
    """
    result = await exchange.set_leverage(request.leverage, request.symbol)
    return result


@app.websocket("/ws/ticker")
async def websocket_ticker(websocket: WebSocket):
    """
    - WebSocket 连接 URL: ws://your_domain:port/ws/ticker?symbol=BTC/USDT&token=your_api_token
    通过 WebSocket 连接实时推送指定交易对的 ticker 数据。
    需要在连接 URL 中提供 symbol 和 token 参数。

    参数 (通过 WebSocket 连接 URL 传递):
    - symbol: 要订阅的交易对，例如 "BTC/USDT"。
    - token: 您的 API token。
    """
    await websocket.accept()
    params = websocket.query_params
    symbol = params.get("symbol")
    token = params.get("token")

    if token != API_TOKEN:
        await websocket.close(code=1008, reason="Invalid token")
        return

    if not symbol:
        await websocket.close(code=1008, reason="Symbol required")
        return

    try:
        while True:
            ticker = await exchange.fetch_ticker(symbol)
            await websocket.send_json({"symbol": symbol, "price": ticker["last"]})
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        print("Client disconnected gracefully")
    except Exception as e:
        print(f"WebSocket error: {str(e)}")
        await websocket.close(code=1011, reason=f"Server error: {str(e)}")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
