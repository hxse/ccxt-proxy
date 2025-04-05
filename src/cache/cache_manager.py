import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
import ccxt.async_support as ccxt
from typing import Optional, Union
import asyncio
import traceback
import math

# 定义缓存目录并确保它存在
CACHE_DIR = Path("data")
CACHE_DIR.mkdir(exist_ok=True)

# 时间周期映射到秒数 (毫秒单位也加入方便计算)
TIMEFRAME_MS = {
    "1m": 60 * 1000,
    "3m": 180 * 1000,
    "5m": 300 * 1000,
    "10m": 600 * 1000,
    "15m": 900 * 1000,
    "30m": 1800 * 1000,
    "1h": 3600 * 1000,
    "2h": 7200 * 1000,
    "4h": 14400 * 1000,
    "6h": 6 * 3600 * 1000,
    "8h": 8 * 3600 * 1000,
    "12h": 12 * 3600 * 1000,
    "1d": 86400 * 1000,
    "2d": 2 * 86400 * 1000,
    "3d": 3 * 86400 * 1000,
    "1w": 604800 * 1000,
    "2w": 2 * 604800 * 1000,
}
TIMEFRAME_SECONDS = {k: v // 1000 for k, v in TIMEFRAME_MS.items()}

OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume", "date"]
EMPTY_OHLCV_DF = pd.DataFrame(columns=OHLCV_COLUMNS)


def _process_ohlcv_dataframe(df_to_process: pd.DataFrame) -> pd.DataFrame:
    """
    处理 OHLCV DataFrame,进行数据类型转换、缺失值处理、去重、排序和添加日期列.

    Args:
        df_to_process: 需要处理的 DataFrame.

    Returns:
        处理后的 DataFrame.
    """
    # print("开始处理 OHLCV DataFrame...")
    if df_to_process.empty:
        # print("DataFrame 为空,返回空的 DataFrame.")
        return EMPTY_OHLCV_DF
    try:
        # print("转换 'timestamp' 列为数值类型并处理缺失值...")
        df_to_process["timestamp"] = pd.to_numeric(
            df_to_process["timestamp"], errors="coerce"
        ).astype("Int64")
        df_to_process = df_to_process.dropna(subset=["timestamp"])
        df_to_process["timestamp"] = df_to_process["timestamp"].astype(int)

        # print("去除重复的时间戳并按时间戳排序...")
        df_to_process = df_to_process.drop_duplicates(
            subset="timestamp", keep="first"
        ).sort_values("timestamp")

        # print("添加 'date' 列...")
        df_to_process["date"] = pd.to_datetime(
            df_to_process["timestamp"], unit="ms", utc=True
        ).dt.strftime("%Y-%m-%d %H:%M:%S+00:00")

        # print("OHLCV DataFrame 处理完成.")

        return df_to_process
    except Exception as e:
        print(f"处理获取到的 OHLCV 数据时出错: {e}")
        print(traceback.format_exc())
        return EMPTY_OHLCV_DF


async def get_ohlcv_data_from_exchange(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since: Optional[int],
    limit: int,
) -> dict:
    """
    获取 OHLCV 数据,返回已完成和可能未完成的 K 线.

    Args:
        exchange: ccxt 交易所实例.
        symbol: 交易对.
        timeframe: 时间周期.
        since: 起始时间戳（毫秒）.
        limit: 请求的 K 线数量.

    Returns:
        一个字典,包含 'done_data' (已完成的 K 线数据 DataFrame) 和 'last_data' (最后一根可能未完成的 K 线数据 DataFrame).

    具体说明:
    函数返回 {"done_data":done_data,"last_data":last_data}
    done_data:DataFrame 只有已完成k线, 没有未完成k线, 要是没有已完成k线就为空
    last_data:DataFrame 只放未完成k线, 不放已完成k线, 要是没有未完成k线就为空

    ccxt和币安, 如果请求历史k线, 只会返回已完成的k线, 但是如果请求包含了最新的k线, 就会包含最新的未完成k线, 由于币安不返回结束时间戳, 所以只能通过超额请求的方法来标注两种k线

    算法说明如下:
    当请求 limit 根 K 线时,程序会尝试获取 limit + 1 根.如果币安返回了 n 根：
    如果返回的 n 根数量足够接近我们请求的数量(n >= limit),我们认为最后返回的那根可能是未完成的,因此将前 n - 1 根视为已完成的 done_data,而 last_data 为空.
    如果返回的 n 根数量比我们请求的少(n < limit),则认为最后返回的那根是最新且可能未完成的,因此将前 n - 1 根视为已完成的 done_data,最后一根作为 last_data."

    算法举例如下:
    假设从时间s开始, 到当前最新k线有limit=10根, 前9根为done, 第10根为last
    如果超额请求一根呢:
    当用户从时间s开始, 请求limit=8根, 那么程序向币安请求limit+1=9根, 程序返回n=9根, 因为9-9=0, 因为小于1, 所以把前8根(n-1)数据放到done_data字段, last_data字段为空
    当用户从时间s开始, 请求limit=9根, 那么程序向币安请求limit+1=10根, 程序返回n=10根, 因为10-10=0, 因为小于1, 所以把前9根(n-1)数据放到done_data字段, last_data字段为空
    当用户从时间s开始, 请求limit=10根, 那么程序向币安请求limit+1=11根, 程序返回n=10根, 因为11-10=1, 因为大于等于1, 所以把前9根(n-1)数据放到done_data字段, 第10根(n)放到last_data字段
    当用户从时间s开始, 请求limit=11根, 那么程序向币安请求limit+1=12根, 程序返回n=10根, 因为12-10=2, 因为大于等于1, 所以把前9根(n-1)数据放到done_data字段, 第10根(n)放到last_data字段

    如果用户只请求一根, 会遇到边界问题吗:
    如果从时间s开始, 到最新的k线有两根, 前1根为done, 第2根为last
    当用户从时间s开始, 请求limit=1根, 那么程序向币安请求limit+1=2根, 程序返回n=2根, 因为2-2=0, 因为小于1, 所以把前1根(n-1)数据放到done_data字段, last_data字段为空
    如果从时间s开始, 到最新的k线有1根, 为last
    当用户从时间s开始, 请求limit=1根, 那么程序向币安请求limit+1=2根, 程序返回n=1根, 因为2-1=1, 因为大于等于1, 所以把前0根(n-1)数据放到done_data字段, 也就是None, 第1根(n)放到last_data字段, 这是符合预期的

    还有就是,币安每次最多只支持请求1000根k线,所以注意分页请求,然后合并去重
    """
    """
    可以修改函数,也可以修改行内注释,但是不要修改文档注释,修改函数要遵循文档函数中的算法逻辑
    不要说什么这段代码保持不变这种话, 如果有修改就返回全部文件或全部函数, 否则会导致我复制不完整代码, 把项目搞乱
    """
    print(
        f"开始从交易所获取数据: 交易对={symbol}, 时间周期={timeframe}, 起始时间={since}, 请求数量={limit}"
    )
    if not exchange.has["fetchOHLCV"]:
        print(f"警告: 交易所 {exchange.id} 不支持 fetchOHLCV 功能.")
        return {"done_data": EMPTY_OHLCV_DF, "last_data": EMPTY_OHLCV_DF}

    tf_ms = TIMEFRAME_MS.get(timeframe)
    if not tf_ms:
        print(f"错误：未知的时间周期 '{timeframe}'")
        return {"done_data": EMPTY_OHLCV_DF, "last_data": EMPTY_OHLCV_DF}

    requested_limit = int(limit)  # 保存原始请求的 limit
    numerical_limit = requested_limit + 1  # 超额请求一根以判断最后一根是否完成
    if numerical_limit <= 0:
        print(f"请求的数量 ({limit}) 为非正数.无需获取.")
        return {"done_data": EMPTY_OHLCV_DF, "last_data": EMPTY_OHLCV_DF}

    ohlcv_list = []
    rate_limit = getattr(exchange, "rateLimit", 1000) / 1000  # 秒
    max_per_request = (
        getattr(exchange, "limits", {}).get("fetchOHLCV", {}).get("max", 1000)
    )
    if max_per_request is None:
        max_per_request = 1000  # 以防万一
    print(
        f"交易所 {exchange.id} 的速率限制为每秒 {rate_limit:.2f} 次请求,单次最大请求数量为 {max_per_request}."
    )
    m = 10
    rate_limit = rate_limit * m if rate_limit <= 0.1 else rate_limit
    print(f"放慢请求速率{m}倍至 {rate_limit}")

    remaining = numerical_limit
    current_since = since

    print(
        f"开始循环获取数据: 总共需要={numerical_limit}, 单次最大={max_per_request}, 起始 since={current_since}"
    )

    while remaining > 0:
        fetch_limit = min(remaining, max_per_request)
        print(
            f"正在获取数据分块: since={current_since}, limit={fetch_limit}, 剩余={remaining}"
        )
        try:
            ohlcv = await exchange.fetch_ohlcv(
                symbol, timeframe, since=current_since, limit=fetch_limit
            )

            if not ohlcv:
                print("交易所没有返回更多数据.")
                break  # 如果没有数据返回则退出循环

            ohlcv_df = pd.DataFrame(
                ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
            ohlcv_df = ohlcv_df.drop_duplicates(subset="timestamp", keep="first")
            ohlcv = ohlcv_df.values.tolist()

            if ohlcv_list and ohlcv:
                last_fetched_ts = ohlcv_list[-1][0]
                original_count = len(ohlcv)
                # 过滤掉时间戳小于等于已获取的最新时间戳的数据,避免重复
                ohlcv = [candle for candle in ohlcv if candle[0] > last_fetched_ts]
                filtered_count = len(ohlcv)
                if original_count > filtered_count:
                    print(
                        f"过滤掉 {original_count - filtered_count} 个重叠的 K 线数据."
                    )

            if not ohlcv:
                print("过滤重叠数据后没有新数据.")
                break

            ohlcv_list.extend(ohlcv)
            actual_fetched_count = len(ohlcv)
            remaining -= actual_fetched_count

            print(f"本次获取了 {actual_fetched_count} 根 K 线.目标剩余 {remaining} 根.")

            if remaining > 0 and actual_fetched_count > 0:
                last_ts = int(ohlcv[-1][0])
                # 设置下一次请求的起始时间为本次获取的最后一个 K 线的时间加上一个时间周期
                current_since = last_ts + tf_ms
                print(f"等待 {rate_limit:.2f} 秒以遵守速率限制...")
                await asyncio.sleep(rate_limit)
            elif actual_fetched_count == 0 and remaining > 0:
                print("交易所返回空列表,但仍需要数据,提前结束获取.")
                break
            else:
                # print("已达到或超过目标数量,结束获取.")
                break

        except ccxt.NetworkError as e:
            print(f"获取 OHLCV 时出现 NetworkError: {e}.休眠后重试...")
            await asyncio.sleep(5)
        except ccxt.RateLimitExceeded as e:
            print(f"获取 OHLCV 时超出速率限制: {e}.将等待更长时间后重试...")
            await asyncio.sleep(60)
        except ccxt.ExchangeError as e:
            print(f"获取 OHLCV 时出现 ExchangeError: {e}.中止获取.")
            break
        except Exception as e:
            print(f"获取 OHLCV 时出现意外错误: {e}")
            print(traceback.format_exc())
            break

    print(f"从交易所获取完成.总共获取 K 线数量: {len(ohlcv_list)}")

    df = pd.DataFrame(
        ohlcv_list, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    done_data_df = EMPTY_OHLCV_DF
    last_data_df = EMPTY_OHLCV_DF

    if not df.empty:
        returned_count = len(df)
        print(
            f"交易所返回了 {returned_count}根K线,原始请求数量为 {requested_limit},超额请求数量为 {numerical_limit}"
        )
        # 根据返回的数量和请求的数量判断最后一根是否是未完成的
        if returned_count == numerical_limit:
            print("交易所返回数量等于超额请求数量")
            done_data_df = df.iloc[:-1].copy()  # 前 n-1 根是已完成的
        elif returned_count < numerical_limit:
            print("交易所返回数量小于超额请求数量")
            done_count = returned_count - 1
            if done_count >= 0:
                print(f"将前 {done_count} 条数据视为已完成的 K 线.")
                done_data_df = df.iloc[:done_count].copy()  # 前 n-1 根是已完成的
            if returned_count > 0:
                print("将最后一条数据视为可能未完成的 K 线.")
                last_data_df = df.iloc[-1:].copy()  # 最后 1 根可能是未完成的

    # print("处理从交易所获取的已完成 K 线数据...")
    done_data_df = _process_ohlcv_dataframe(done_data_df)
    # print("处理从交易所获取的可能未完成的 K 线数据...")
    last_data_df = _process_ohlcv_dataframe(last_data_df)

    print("返回从交易所获取的数据.")
    return {"done_data": done_data_df, "last_data": last_data_df}


def save_ohlcv_to_file(file_path: Path, df: pd.DataFrame):
    print(f"尝试保存 OHLCV 数据到文件: {file_path}")
    if not df.empty:
        df["timestamp"] = df["timestamp"].astype(int)
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["open", "high", "low", "close", "volume"])
        if not df.empty:
            df["date"] = pd.to_datetime(
                df["timestamp"], unit="ms", utc=True
            ).dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
            print(f"保存 {len(df)} 行数据到 {file_path}...")
            df.to_csv(file_path, index=False, encoding="utf-8")
            print(f"已成功保存 {len(df)} 行到 {file_path} (包含 date 列)")
        else:
            print(f"数据在类型转换和 NaN 处理后为空,未保存文件: {file_path}")
    else:
        print(f"尝试保存空 DataFrame 到 {file_path}.已跳过.")


def read_ohlcv_from_file(file_path: Path) -> pd.DataFrame:
    # print(f"尝试从文件读取 OHLCV 数据: {file_path}")
    if not file_path.exists():
        print(f"缓存文件未找到: {file_path}")
        return EMPTY_OHLCV_DF
    try:
        # print(f"读取 CSV 文件: {file_path}")
        df = pd.read_csv(file_path)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype(
                "Int64"
            )
            df = df.dropna(subset=["timestamp"])
            df["timestamp"] = df["timestamp"].astype(int)
            df["date"] = pd.to_datetime(
                df["timestamp"], unit="ms", utc=True
            ).dt.strftime("%Y-%m-%d %H:%M:%S+00:00")
        else:
            print(f"警告：缓存文件 {file_path} 缺少 'timestamp' 列.")
            return EMPTY_OHLCV_DF

        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # print(f"已成功从 {file_path} 读取了 {len(df)} 行 (包含 date 列)")
        return df
    except pd.errors.EmptyDataError:
        print(f"缓存文件为空: {file_path}")
        return EMPTY_OHLCV_DF
    except Exception as e:
        print(f"读取缓存文件 {file_path} 出错: {e}")
        return EMPTY_OHLCV_DF


def _check_continuity(df: pd.DataFrame, timeframe: str):
    print(f"检查 DataFrame 的连续性,时间周期为: {timeframe}")
    if len(df) < 2:
        print("DataFrame 中少于 2 行,视为连续.")
        return True
    expected_interval = TIMEFRAME_MS.get(timeframe)
    if expected_interval is None:
        print(f"警告：未知的时间周期 '{timeframe}',无法进行连续性检查.")
        return True
    timestamps = df["timestamp"].sort_values().tolist()
    for i in range(len(timestamps) - 1):
        if timestamps[i + 1] - timestamps[i] != expected_interval:
            print(
                f"发现不连续性：时间戳 {timestamps[i + 1]} 和 {timestamps[i]} 之间的间隔不是预期的 {expected_interval} 毫秒."
            )
            return False
    print("DataFrame 中的时间戳是连续的.")
    return True


def _save_new_data_to_cache(
    df: pd.DataFrame, parent_dir: Path, symbol_norm: str, timeframe: str
):
    print(
        f"尝试保存新数据到缓存,目录: {parent_dir}, 交易对: {symbol_norm}, 时间周期: {timeframe}"
    )
    if not df.empty:
        print("检查新数据的连续性...")
        if _check_continuity(df, timeframe):
            new_cache_start_time = df["timestamp"].min()
            new_cache_start_dt = datetime.fromtimestamp(
                new_cache_start_time / 1000, timezone.utc
            )
            new_date_str = new_cache_start_dt.strftime("%Y%m%d %H%M%S")
            new_cache_file = (
                parent_dir / f"{symbol_norm}_{timeframe}_{new_date_str}.csv"
            )
            save_ohlcv_to_file(new_cache_file, df)
            print(f"新获取的 done_data 已保存到缓存文件: {new_cache_file}")
        else:
            print("警告：保存到缓存前发现数据不连续,跳过保存.")
    else:
        print("尝试保存的 DataFrame 为空,跳过保存.")


# --- 缓存处理逻辑 ---
async def handle_ohlcv_cache(
    mode: str,
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since: Optional[int],
    limit: int,
) -> dict:
    """
    函数返回最终对象 {"done_data":done_data,"last_data":last_data}
    done_data: DataFrame 只有已完成k线, 没有未完成k线, 要是没有已完成k线就为空
    last_data: DataFrame 只放未完成k线, 不放已完成k线, 要是没有未完成k线就为空

    本地缓存文件中, 只接受读取和保存done_data数据, last_data不要保存进去
    last_data在函数中不进行任何处理, 在函数最后随着done_data一起返回就行

    时间戳转换一律用utc时间

    缓存文件名示例
    名字中的时间是缓存开始时间, 和文件内容第一行时间相同
    品种_周期_时间_.csv
    BTC_USDT_15m_20230201 000000.csv

    缓存文件内容示例
    timestamp,open,high,low,close,volume,date
    1675209600000,23119.5,23161.4,23027.8,23084.6,7865.032,2023-02-01 00:00:00+00:00
    1675210500000,23084.6,23087.6,23022.5,23034.9,2630.741,2023-02-01 00:15:00+00:00

    用户请求后, 首先查找缓存文件, 先广泛查找, 如果缓存文件开始时间小于等于用户请求时间则符合条件, 如果找到了多个就选择时间最近的文件

    如果没有命中任何缓存文件, 则请求数据get_ohlcv_data, 然后把done_data保存到缓存中, 然后直接返回最终对象, 函数结束

    如果命中了缓存文件, 就看缓存中的k线数量, 是否满足用户请求的k线数量(limit)要求, 满足的话, 直接返回最终对象, 函数结束
    注意满足k线数量要求,不是计算整个缓存数据的数量,而是从用户请求时间开始,计算到缓存最后一根

    如果命中了缓存文件, 还要进行边界检查, 如果空隙时间>0并且空隙时间大于一根K线的间隔空隙, 视为无法合并到缓存中, 处理方法跳转到上述的没有命中任何缓存文件
    空隙时间 = 用户请求时间 - 缓存文件中最后一根K线的开始时间

    如果不满足的话, 就请求get_ohlcv_data, 然后把done_data和缓存中的done_data合并, 并且保存到缓存中, 然后把对象最终返回, 函数结束
    """
    print(
        f"开始处理缓存 (模式: {mode}): 交易对={symbol}, 时间周期={timeframe}, 起始时间={since}, 请求数量={limit}"
    )
    tf_ms = TIMEFRAME_MS.get(timeframe)
    if not tf_ms:
        print(f"错误：未知的时间周期 '{timeframe}'")
        return {"done_data": EMPTY_OHLCV_DF, "last_data": EMPTY_OHLCV_DF}

    symbol_norm = symbol.replace("/", "_")  # 规范化交易对名称以便兼容文件系统
    parent_dir = Path(CACHE_DIR / mode / symbol_norm / timeframe)
    parent_dir.mkdir(parents=True, exist_ok=True)
    print(f"缓存文件目录: {parent_dir}")

    original_since = since  # 记录原始请求的 since
    original_limit = limit  # 记录原始请求的 limit
    # print(f"原始请求参数: since={original_since}, limit={original_limit}")

    # 如果未提供 since,则根据 limit 确定默认值
    if since is None:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        if isinstance(limit, int) and limit > 0:
            since = now_ms - (limit * tf_ms)
            since = (since // tf_ms) * tf_ms
            print(
                f"由于 since 为 None, limit 为 {limit}, 计算得到的 since: {since} ({datetime.fromtimestamp(since / 1000, timezone.utc)})"
            )
        else:  # limit 是无效的或 <= 0
            default_limit = 1000
            print(
                f"警告: 无效或非正数 limit '{limit}' 且 since=None.计算默认 {default_limit} 根 K 线的 since."
            )
            since = now_ms - (default_limit * tf_ms)
            since = (since // tf_ms) * tf_ms
        print(f"最终确定的起始时间 (since): {since}")
    # else:
    #     print(
    #         f"使用提供的起始时间 (since): {since} ({datetime.fromtimestamp(since / 1000, timezone.utc)})"
    #     )

    print(
        f"处理缓存 (最终参数): {symbol}, {timeframe}, since={since}, limit={original_limit}"
    )

    # --- 查找缓存文件 ---
    # print("开始查找相关的缓存文件...")
    cache_files = sorted(parent_dir.glob(f"{symbol_norm}_{timeframe}_*.csv"))
    valid_files = []
    if cache_files and since is not None:
        # print(f"找到 {len(cache_files)} 个缓存文件,开始筛选...")
        for f in cache_files:
            try:
                parts = f.stem.split("_")
                if len(parts) >= 3:
                    file_date_str = parts[-1]
                    file_since_dt = datetime.strptime(
                        file_date_str, "%Y%m%d %H%M%S"
                    ).replace(tzinfo=timezone.utc)
                    file_since_ms = int(file_since_dt.timestamp() * 1000)

                    # 检查缓存文件的开始时间是否小于等于用户请求的起始时间
                    if file_since_ms <= since:
                        valid_files.append((f, file_since_ms))
                    #     print(
                    #         f"缓存文件 {f.name} 的开始时间 ({datetime.fromtimestamp(file_since_ms / 1000, timezone.utc)}) 小于等于请求的起始时间,视为有效."
                    #     )
                    # else:
                    #     print(
                    #         f"跳过开始时间晚于请求的缓存文件: {f.name} ({datetime.fromtimestamp(file_since_ms / 1000, timezone.utc)} > {datetime.fromtimestamp(since / 1000, timezone.utc)})"
                    #     )
                    # else:
                    #     print(f"跳过名称格式不符合预期的缓存文件: {f.name}")
            except ValueError:
                print(f"无法从缓存文件名解析日期: {f.name}.跳过.")
            except Exception as e:
                print(f"处理缓存文件 {f.name} 时出错: {e}.跳过")

    print(
        f"找到 {len(cache_files)} 个缓存文件.找到 {len(valid_files)} 个相关的缓存文件 (开始时间 <= {since})."
    )

    # --- 基于缓存可用性的逻辑 ---
    latest_cache_path = None
    cached_done_data = EMPTY_OHLCV_DF
    cache_used = False  # 标记是否使用了缓存数据

    if valid_files:
        latest_cache_path, cache_start_ms = max(valid_files, key=lambda x: x[1])
        print(
            f"选择最新的有效缓存: {latest_cache_path} (文件开始于 {datetime.fromtimestamp(cache_start_ms / 1000, timezone.utc)})"
        )
        cached_done_data = read_ohlcv_from_file(latest_cache_path)

        if not cached_done_data.empty:
            cache_used = True
            print(f"缓存文件包含 {len(cached_done_data)} 行数据.")
            # 如果缓存中的数据量满足用户请求的 limit,则直接返回缓存数据
            # 修改开始 >>>
            relevant_cache_data = cached_done_data[
                cached_done_data["timestamp"] >= since
            ]
            if len(relevant_cache_data) >= original_limit:
                print(
                    f"从请求时间{since}开始,缓存数据量 {len(relevant_cache_data)} 满足请求的 {original_limit} 根."
                )
                return {
                    "done_data": relevant_cache_data.head(original_limit).copy(),
                    "last_data": EMPTY_OHLCV_DF,
                }
            else:
                print(
                    f"从请求时间开始{since},缓存数据量 ({len(relevant_cache_data)}) 不满足请求的 {original_limit} 根,需要获取更多数据."
                )
            # 修改结束 <<<
            # 检查缓存中最后一条数据的时间戳与用户请求起始时间之间是否存在较大的间隔
            last_cache_ts = cached_done_data["timestamp"].max()
            gap_ms = since - last_cache_ts if since > last_cache_ts else 0
            # 如果间隔大于一个时间周期,则认为缓存不连续,需要重新从交易所获取
            if gap_ms > tf_ms:
                print(f"检测到 {gap_ms / tf_ms:.2f} 个时间周期的空隙,视为缓存未命中.")
                cache_used = False
                cached_done_data = EMPTY_OHLCV_DF  # 重置缓存数据
            else:
                # 计算需要从交易所获取的数据量和起始时间
                fetch_since = last_cache_ts + tf_ms
                fetch_limit = original_limit - len(relevant_cache_data)
                print(
                    f"从 {datetime.fromtimestamp(fetch_since / 1000, timezone.utc)} 开始获取 {fetch_limit} 根新数据."
                )
                exchange_data = await get_ohlcv_data_from_exchange(
                    exchange, symbol, timeframe, fetch_since, fetch_limit
                )
                new_done_data = exchange_data["done_data"]
                last_data = exchange_data["last_data"]

                # 合并缓存数据和新获取的数据
                if not new_done_data.empty:
                    combined_done_data = pd.concat(
                        [cached_done_data, new_done_data], ignore_index=True
                    )
                    combined_done_data = combined_done_data.drop_duplicates(
                        subset="timestamp", keep="first"
                    ).sort_values("timestamp")
                    # 检查合并后的数据是否连续
                    if _check_continuity(combined_done_data, timeframe):
                        save_ohlcv_to_file(latest_cache_path, combined_done_data)
                        print(f"缓存文件 {latest_cache_path} 已更新并合并了新数据.")
                        return {
                            "done_data": combined_done_data.head(original_limit).copy(),
                            "last_data": last_data,
                        }
                    else:
                        print("警告：合并后数据不连续,不更新缓存.")
                        return {
                            "done_data": cached_done_data.head(original_limit).copy(),
                            "last_data": last_data,  # 返回刚刚获取的 last_data
                        }
                else:
                    print("获取新数据后 done_data 为空,返回缓存数据和last_data")
                    return {
                        "done_data": cached_done_data.head(original_limit).copy(),
                        "last_data": last_data,  # 没有done数据,也可能有last数据
                    }
        else:
            print(f"读取缓存文件 {latest_cache_path} 为空.")
            cache_used = False

    # --- 没有命中任何缓存文件或缓存不满足条件 ---
    if not cache_used:
        print("没有找到合适的缓存文件或缓存不满足条件,从交易所获取全新数据.")
        exchange_result = await get_ohlcv_data_from_exchange(
            exchange, symbol, timeframe, since, original_limit
        )
        fetched_done_data = exchange_result["done_data"]
        fetched_last_data = exchange_result["last_data"]

        _save_new_data_to_cache(fetched_done_data, parent_dir, symbol_norm, timeframe)

        return {"done_data": fetched_done_data, "last_data": fetched_last_data}

    # 理论上不应该执行到这里
    return {"done_data": EMPTY_OHLCV_DF, "last_data": EMPTY_OHLCV_DF}
