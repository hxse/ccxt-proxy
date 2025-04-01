import json
import os
import asyncio
import ccxt.async_support as ccxt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict
from datetime import datetime
import uvicorn
import websockets
from fastapi.middleware.cors import CORSMiddleware
from cache.cache_manager import (
    handle_ohlcv_cache,
    fetch_ohlcv_by_period,
    get_ohlcv_data,
)
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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

# 初始化 FastAPI 和 CCXT
app = FastAPI()


origins = [
    "*",
    "http://localhost",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

mode = "test" if len(sys.argv) > 1 and sys.argv[1] == "test" else "live"
http_proxy = config["proxy"].get("http", None)
https_proxy = config["proxy"].get("https", None)
print(http_proxy, https_proxy)
exchange = ccxt.binance(
    {
        "apiKey": config["binance"][mode].get("api_key"),
        "secret": config["binance"][mode].get("secret"),
        "enableRateLimit": True,
        "options": {"defaultType": config["type"]},
        # "proxies": {
        #     "http": http_proxy,
        #     "https": https_proxy,
        # },
        "verbose": True,  # 启用详细日志
    }
)
exchange.httpProxy = http_proxy
exchange.httpsProxy = http_proxy
exchange.wsProxy = http_proxy

if mode == "test":
    exchange.set_sandbox_mode(True)


# 时间周期转换为秒
TIMEFRAME_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


# 验证 Token 的依赖
def verify_token(token: str):
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid token")
    return token


async def handle_exceptions():
    try:
        yield
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{e}")


# 请求模型
class OHLCVRequest(BaseModel):
    symbol: str
    timeframe: str
    since: Optional[int] = None
    limit: Optional[int] = 100
    cache: bool = True


class PeriodRequest(BaseModel):
    symbol: str
    timeframe: str
    start_date: str  # YYYY-MM-DD
    periods: int  # 天数或月数
    cache: bool = True


class OrderRequest(BaseModel):
    symbol: str
    side: str  # "buy" or "sell"
    amount: float
    price: Optional[float] = None  # 市价单可为空
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
    distance: float  # 跟踪距离


class LeverageRequest(BaseModel):
    symbol: str
    leverage: int


# RESTful API 路由


@app.get("/hello")
async def get_hello(token: str = Depends(verify_token)):
    return {"hello": "world"}


@app.get("/api/ohlcv")
async def get_ohlcv(
    request: OHLCVRequest = Depends(),
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """获取 OHLCV 数据（起始时间 + 数量模式）

    示例:
    - GET /api/ohlcv?symbol=BTC/USDT&timeframe=15m&since=1677657600000&limit=100&cache=true&token=your_token
    """
    if request.timeframe not in TIMEFRAME_SECONDS:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")

    print(request.cache, request.symbol)
    if request.cache:
        ohlcv = await handle_ohlcv_cache(
            mode,
            exchange,
            request.symbol,
            request.timeframe,
            request.since,
            request.limit,
        )
        print(
            "cache",
        )
    else:
        print(
            "no cache",
        )
        ohlcv = await get_ohlcv_data(
            exchange,
            request.symbol,
            request.timeframe,
            request.since,
            request.limit,
        )
    return {"symbol": request.symbol, "ohlcv": ohlcv}


@app.get("/api/ohlcv_period")
async def get_ohlcv_period(
    request: PeriodRequest = Depends(),
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """获取 OHLCV 数据（连续周期模式）

    示例:
    - GET /api/ohlcv_period?symbol=BTC/USDT&timeframe=15m&start_date=2023-03-01&periods=3&cache=true&token=your_token
    """
    if request.timeframe not in TIMEFRAME_SECONDS:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")
    print(type(request.cache), request.cache)
    if request.cache:
        ohlcv = await fetch_ohlcv_by_period(
            exchange,
            request.symbol,
            request.timeframe,
            request.start_date,
            request.periods,
        )
    else:
        ohlcv = await get_ohlcv_data(
            exchange,
            request.symbol,
            request.timeframe,
            request.start_date,
            request.periods,
        )
    return {"symbol": request.symbol, "ohlcv": ohlcv}


@app.get("/api/ticker")
async def get_ticker(
    symbol: str,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """获取最新价格（RESTful 备用接口）

    示例:
    - GET /api/ticker?symbol=BTC/USDT&token=your_token
    """
    ticker = await exchange.fetch_ticker(symbol)
    return {"symbol": symbol, "price": ticker["last"]}


@app.post("/api/order")
async def create_order(
    request: OrderRequest,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """创建市价/限价单（支持止盈止损）

    示例:
    - POST /api/order?token=your_token
      Body: {"symbol": "BTC/USDT", "side": "buy", "amount": 0.1, "price": 50000, "take_profit": 55000, "stop_loss": 45000}
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
    """创建市价止盈止损单（触发价格）

    示例:
    - POST /api/stop_order?token=your_token
      Body: {"symbol": "BTC/USDT", "side": "buy", "amount": 0.1, "trigger_price": 50000}
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
    """创建跟踪止损单

    示例:
    - POST /api/trailing_stop?token=your_token
      Body: {"symbol": "BTC/USDT", "side": "buy", "amount": 0.1, "distance": 1000}
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
    """取消订单

    示例:
    - POST /api/cancel_order?order_id=12345&symbol=BTC/USDT&token=your_token
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
    """查询订单

    示例:
    - GET /api/order?order_id=12345&symbol=BTC/USDT&token=your_token
    """
    order = await exchange.fetch_order(order_id, symbol)
    return order


@app.get("/api/positions")
async def fetch_positions(
    symbol: Optional[str] = None,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """查询当前持仓

    示例:
    - GET /api/positions?symbol=BTC/USDT&token=your_token
    """
    positions = await exchange.fetch_positions(symbol)
    return positions


@app.get("/api/history_positions")
async def fetch_history_positions(
    symbol: Optional[str] = None,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """查询历史持仓

    示例:
    - GET /api/history_positions?symbol=BTC/USDT&token=your_token
    """
    positions = await exchange.fetch_closed_positions(symbol)
    return positions


@app.get("/api/open_orders")
async def fetch_open_orders(
    symbol: Optional[str] = None,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """查询当前委托

    示例:
    - GET /api/open_orders?symbol=BTC/USDT&token=your_token
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
    """查询历史委托

    示例:
    - GET /api/history_orders?symbol=BTC/USDT&since=1677657600000&limit=50&token=your_token
    """
    orders = await exchange.fetch_closed_orders(symbol, since, limit)
    return orders


@app.get("/api/balance")
async def fetch_balance(
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """查询余额

    示例:
    - GET /api/balance?token=your_token
    """
    balance = await exchange.fetch_balance()
    return balance


@app.post("/api/leverage")
async def set_leverage(
    request: LeverageRequest,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """设置杠杆

    示例:
    - POST /api/leverage?token=your_token
      Body: {"symbol": "BTC/USDT", "leverage": 10}
    """
    result = await exchange.set_leverage(request.leverage, request.symbol)
    return result


@app.get("/api/leverage")
async def get_leverage(
    symbol: str,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """查询杠杆

    示例:
    - GET /api/leverage?symbol=BTC/USDT&token=your_token
    """
    leverage = await exchange.fetch_leverage(symbol)
    return {"symbol": symbol, "leverage": leverage}


@app.websocket("/ws/ticker")
async def websocket_ticker(websocket: WebSocket):
    """订阅最新价格

    示例:
    - ws://localhost:8000/ws/ticker?symbol=BTC/USDT&token=your_token
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
        print("Client disconnected gracefully")  # 记录日志，不再关闭
    except Exception as e:
        print(f"WebSocket error: {str(e)}")
        await websocket.close(code=1011, reason=f"Server error: {str(e)}")


if __name__ == "__main__":
    uvicorn.run("src.main:app", host="0.0.0.0", port=8000, reload=True)
