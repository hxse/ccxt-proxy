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
from cache.cache_manager import handle_ohlcv_cache, get_ohlcv_data, TIMEFRAME_SECONDS
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
    limit: Optional[Union[int, str]] = 100  # 支持数字或 "all"
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
    print(since, type(since))
    if since is None:
        return None

    if isinstance(since, int):
        return since

    if isinstance(since, str):
        # 尝试将字符串转换为整数时间戳
        if since.isdigit():
            return int(since)

        # 尝试解析指定的格式 "YYYY-MM-DD HH:MM:SS" 并假设为 UTC
        try:
            dt = datetime.strptime(since, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=timezone.utc
            )
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
    return {"hello": "world"}


@app.get("/api/ohlcv")
async def get_ohlcv(
    request: OHLCVRequest = Depends(),
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    """获取 OHLCV 数据（支持 ISO 时间戳或整数时间戳）

    示例:
    - GET /api/ohlcv?symbol=BTC/USDT&timeframe=15m&since=2023-01-01T00:00:00Z&limit=100&cache=true&token=your_token
    - GET /api/ohlcv?symbol=BTC/USDT&timeframe=15m&since=1677657600000&limit=all&cache=true&token=your_token
    """
    if request.timeframe not in TIMEFRAME_SECONDS:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")

    since = parse_since(request.since)

    if request.cache:
        df = await handle_ohlcv_cache(
            mode, exchange, request.symbol, request.timeframe, since, request.limit
        )
        print("cache")
    else:
        print("no cache")
        df = await get_ohlcv_data(
            exchange, request.symbol, request.timeframe, since, request.limit
        )

    return {"symbol": request.symbol, "ohlcv": df.values.tolist()}


@app.get("/api/ticker")
async def get_ticker(
    symbol: str,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    ticker = await exchange.fetch_ticker(symbol)
    return {"symbol": symbol, "price": ticker["last"]}


# 其他路由保持不变
@app.post("/api/order")
async def create_order(
    request: OrderRequest,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
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
    result = await exchange.cancel_order(order_id, symbol)
    return result


@app.get("/api/order")
async def fetch_order(
    order_id: str,
    symbol: str,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    order = await exchange.fetch_order(order_id, symbol)
    return order


@app.get("/api/positions")
async def fetch_positions(
    symbol: Optional[str] = None,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    positions = await exchange.fetch_positions(symbol)
    return positions


@app.get("/api/history_positions")
async def fetch_history_positions(
    symbol: Optional[str] = None,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    positions = await exchange.fetch_closed_positions(symbol)
    return positions


@app.get("/api/open_orders")
async def fetch_open_orders(
    symbol: Optional[str] = None,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
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
    orders = await exchange.fetch_closed_orders(symbol, since, limit)
    return orders


@app.get("/api/balance")
async def fetch_balance(
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    balance = await exchange.fetch_balance()
    return balance


@app.post("/api/leverage")
async def set_leverage(
    request: LeverageRequest,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    result = await exchange.set_leverage(request.leverage, request.symbol)
    return result


@app.get("/api/leverage")
async def get_leverage(
    symbol: str,
    token: str = Depends(verify_token),
    exception_handler: None = Depends(handle_exceptions),
):
    leverage = await exchange.fetch_leverage(symbol)
    return {"symbol": symbol, "leverage": leverage}


@app.websocket("/ws/ticker")
async def websocket_ticker(websocket: WebSocket):
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
