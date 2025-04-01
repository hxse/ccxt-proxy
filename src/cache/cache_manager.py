import csv
import aiofiles
from pathlib import Path
from datetime import datetime, timedelta
import ccxt.async_support as ccxt
from typing import List, Optional

CACHE_DIR = Path("data")
CACHE_DIR.mkdir(exist_ok=True)

TIMEFRAME_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


async def save_ohlcv_to_file(file_path: Path, ohlcv: List[List[float]]):
    async with aiofiles.open(file_path, mode="w", encoding="utf-8", newline="\n") as f:
        writer = csv.writer(f)
        await writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for row in ohlcv:
            await writer.writerow(row)


async def read_ohlcv_from_file(
    file_path: Path, since: Optional[int] = None, limit: Optional[int] = None
) -> List[List[float]]:
    async with aiofiles.open(file_path, mode="r", encoding="utf-8", newline="") as f:
        lines = await f.readlines()
        ohlcv = [
            list(map(float, line.strip().split(","))) for line in lines[1:]
        ]  # 跳过表头
        if since:
            ohlcv = [row for row in ohlcv if row[0] >= since]
        if limit:
            ohlcv = ohlcv[:limit]
        return ohlcv


async def get_ohlcv_data(exchange, symbol, timeframe, since, limit):
    ohlcv = await exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)
    return ohlcv


async def handle_ohlcv_cache(
    mode: str,
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since: Optional[int],
    limit: Optional[int],
) -> List[List[float]]:
    tf_seconds = TIMEFRAME_SECONDS[timeframe]
    is_short_tf = tf_seconds < 3600
    cache_period = 86400 if is_short_tf else 2592000

    start_dt = datetime.utcfromtimestamp(
        (since or int(datetime.utcnow().timestamp() * 1000 - limit * tf_seconds * 1000))
        // 1000
    )
    base_dt = (
        datetime(start_dt.year, start_dt.month, start_dt.day)
        if is_short_tf
        else datetime(start_dt.year, start_dt.month, 1)
    )
    base_time = int(base_dt.timestamp() * 1000)

    period_label = "day" if is_short_tf else "month"
    date_str = (
        base_dt.strftime("%Y-%m-%d") if is_short_tf else base_dt.strftime("%Y-%m-01")
    )

    parentDir = Path(CACHE_DIR / mode / symbol.replace("/", "_") / timeframe)
    parentDir.mkdir(parents=True, exist_ok=True)
    cache_file = (
        parentDir
        / f"{symbol.replace('/', '_')}_{timeframe}_{period_label}_{date_str}.csv"
    )
    update_file = (
        parentDir
        / f"{symbol.replace('/', '_')}_{timeframe}_{period_label}_{date_str}_update.csv"
    )

    if cache_file.exists():
        ohlcv = await read_ohlcv_from_file(cache_file, since, limit)
        if len(ohlcv) == limit:
            return ohlcv
    elif update_file.exists():
        ohlcv = await read_ohlcv_from_file(update_file, since, limit)
        if len(ohlcv) == limit:
            return ohlcv

    ohlcv = await get_ohlcv_data(exchange, symbol, timeframe, since, limit)
    total_seconds = (ohlcv[-1][0] - ohlcv[0][0]) / 1000 if ohlcv else 0
    target_file = cache_file if total_seconds >= cache_period else update_file
    await save_ohlcv_to_file(target_file, ohlcv)

    if target_file == update_file and total_seconds >= cache_period:
        await save_ohlcv_to_file(cache_file, ohlcv)
        update_file.unlink()  # 删除 update 文件

    return ohlcv


async def fetch_ohlcv_by_period(
    exchange: ccxt.Exchange, symbol: str, timeframe: str, start_date: str, periods: int
) -> List[List[float]]:
    tf_seconds = TIMEFRAME_SECONDS[timeframe]
    is_short_tf = tf_seconds < 3600
    base_dt = datetime.strptime(start_date, "%Y-%m-%d")
    period_delta = timedelta(days=1) if is_short_tf else timedelta(days=30)

    all_ohlcv = []
    for i in range(periods):
        current_dt = base_dt + period_delta * i
        date_str = (
            current_dt.strftime("%Y-%m-%d")
            if is_short_tf
            else current_dt.strftime("%Y-%m-01")
        )
        cache_file = (
            CACHE_DIR
            / f"{symbol.replace('/', '_')}_{timeframe}_{'day' if is_short_tf else 'month'}_{date_str}.csv"
        )

        if cache_file.exists():
            ohlcv = await read_ohlcv_from_file(cache_file)
        else:
            since = int(current_dt.timestamp() * 1000)
            limit = int((86400 if is_short_tf else 2592000) / tf_seconds)
            ohlcv = await get_ohlcv_data(exchange, symbol, timeframe, since, limit)
            await save_ohlcv_to_file(cache_file, ohlcv)
        all_ohlcv.extend(ohlcv)

    return all_ohlcv
