import pandas as pd
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
    "15m": 900 * 1000,
    "30m": 1800 * 1000,
    "1h": 3600 * 1000,
    "4h": 14400 * 1000,
    "1d": 86400 * 1000,
    "1w": 604800 * 1000,
}
TIMEFRAME_SECONDS = {k: v // 1000 for k, v in TIMEFRAME_MS.items()}

OHLCV_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume", "date"]
EMPTY_OHLCV_DF = pd.DataFrame(columns=OHLCV_COLUMNS)


# --- 文件读写函数 ---
def save_ohlcv_to_file(file_path: Path, df: pd.DataFrame):
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
            df.to_csv(file_path, index=False, encoding="utf-8")
            print(f"已保存 {len(df)} 行到 {file_path} (包含 date 列)")
        else:
            print(f"数据在类型转换和NaN处理后为空，未保存文件: {file_path}")
    else:
        print(f"尝试保存空 DataFrame 到 {file_path}。已跳过。")


def read_ohlcv_from_file(file_path: Path) -> pd.DataFrame:
    if not file_path.exists():
        print(f"缓存文件未找到: {file_path}")
        return EMPTY_OHLCV_DF
    try:
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
            print(f"警告：缓存文件 {file_path} 缺少 'timestamp' 列。")
            return EMPTY_OHLCV_DF

        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        print(f"从 {file_path} 读取了 {len(df)} 行 (包含 date 列)")
        return df
    except pd.errors.EmptyDataError:
        print(f"缓存文件为空: {file_path}")
        return EMPTY_OHLCV_DF
    except Exception as e:
        print(f"读取缓存文件 {file_path} 出错: {e}")
        return EMPTY_OHLCV_DF


# --- 数据获取函数 ---
async def get_ohlcv_data(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since: Optional[int],
    limit: int,
    done: bool = True,  # 添加 done 参数，默认为 True
) -> pd.DataFrame:
    """
    done为True,会多请求一根K线,然后再删除最后一根K线,这是为了保证只获取到已完成的K线
    """
    print(
        f"正在获取数据: {symbol}, {timeframe}, since={since}, limit={limit}, done={done}"
    )
    if not exchange.has["fetchOHLCV"]:
        print(f"交易所 {exchange.id} 不支持 fetchOHLCV。")
        return EMPTY_OHLCV_DF

    tf_ms = TIMEFRAME_MS.get(timeframe)
    if not tf_ms:
        print(f"错误：未知的时间周期 '{timeframe}'")
        return EMPTY_OHLCV_DF

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    effective_since = since  # 记录原始 since 以便需要时计算

    numerical_limit: int
    requested_limit = int(limit)  # 保存原始请求的 limit
    numerical_limit = requested_limit + 1 if done else requested_limit
    if numerical_limit <= 0:
        print(f"请求的 limit ({limit}) 为非正数。无需获取。")
        return EMPTY_OHLCV_DF

    ohlcv_list = []
    rate_limit = getattr(exchange, "rateLimit", 1000) / 1000  # 秒
    max_per_request = (
        getattr(exchange, "limits", {}).get("fetchOHLCV", {}).get("max", 1000)
    )
    if max_per_request is None:
        max_per_request = 1000  # 以防万一

    remaining = numerical_limit
    current_since = effective_since

    print(
        f"开始获取循环: 总共需要={numerical_limit}, 单次最大={max_per_request}, 起始 since={current_since}"
    )

    while remaining > 0:
        fetch_limit = min(remaining, max_per_request)
        print(
            f"正在获取分块: since={current_since}, limit={fetch_limit}, 剩余={remaining}"
        )
        try:
            ohlcv = await exchange.fetch_ohlcv(
                symbol, timeframe, since=current_since, limit=fetch_limit
            )

            if not ohlcv:
                print("交易所没有返回更多数据。")
                break  # 如果没有数据返回则退出循环

            ohlcv_df = pd.DataFrame(
                ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
            ohlcv_df = ohlcv_df.drop_duplicates(subset="timestamp", keep="first")
            ohlcv = ohlcv_df.values.tolist()

            if ohlcv_list and ohlcv:
                last_fetched_ts = ohlcv_list[-1][0]
                original_count = len(ohlcv)
                ohlcv = [candle for candle in ohlcv if candle[0] > last_fetched_ts]
                filtered_count = len(ohlcv)
                if original_count > filtered_count:
                    print(
                        f"过滤掉 {original_count - filtered_count} 个重叠的 K 线数据。"
                    )

            if not ohlcv:
                print("过滤重叠数据后没有新数据。")
                break

            ohlcv_list.extend(ohlcv)
            actual_fetched_count = len(ohlcv)
            remaining -= actual_fetched_count

            print(
                f"本次获取了 {actual_fetched_count} 根 K 线。目标剩余 {remaining} 根。"
            )

            if remaining > 0 and actual_fetched_count > 0:
                last_ts = int(ohlcv[-1][0])
                current_since = last_ts + tf_ms
                await asyncio.sleep(rate_limit)
            elif actual_fetched_count == 0 and remaining > 0:
                print("交易所返回空列表，但仍需要数据，提前结束获取。")
                break
            else:
                print("已达到或超过目标数量，结束获取。")
                break

        except ccxt.NetworkError as e:
            print(f"获取 OHLCV 时出现 NetworkError: {e}。休眠后重试...")
            await asyncio.sleep(5)
        except ccxt.RateLimitExceeded as e:
            print(f"获取 OHLCV 时超出速率限制: {e}。将等待更长时间后重试...")
            await asyncio.sleep(60)
        except ccxt.ExchangeError as e:
            print(f"获取 OHLCV 时出现 ExchangeError: {e}。中止获取。")
            break
        except Exception as e:
            print(f"获取 OHLCV 时出现意外错误: {e}")
            print(traceback.format_exc())
            break

    print(f"获取完成。总共获取 K 线数量: {len(ohlcv_list)}")

    if not ohlcv_list:
        return EMPTY_OHLCV_DF

    df = pd.DataFrame(
        ohlcv_list, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )

    if done and not df.empty:
        df = df.iloc[:-1]
        print("移除了最后一根 K 线，因为 done=True。")
    if len(df) < requested_limit:
        print(
            f"警告: K线返回的数据量 ({len(df)}) 小于请求的 limit ({requested_limit})。"
        )

    try:
        df["timestamp"] = pd.to_numeric(df["timestamp"], errors="coerce").astype(
            "Int64"
        )
        df = df.dropna(subset=["timestamp"])
        df["timestamp"] = df["timestamp"].astype(int)
        for col in ["open", "high", "low", "close", "volume"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df = df.drop_duplicates(subset="timestamp", keep="first").sort_values(
            "timestamp"
        )
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.strftime(
            "%Y-%m-%d %H:%M:%S+00:00"
        )

        return df
    except Exception as e:
        print(f"处理获取到的 OHLCV 数据时出错: {e}")
        print(traceback.format_exc())
        return EMPTY_OHLCV_DF


# --- 缓存处理逻辑 ---
async def handle_ohlcv_cache(
    mode: str,
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since: Optional[int],
    limit: int,
) -> pd.DataFrame:
    tf_ms = TIMEFRAME_MS.get(timeframe)
    if not tf_ms:
        print(f"错误：未知的时间周期 '{timeframe}'")
        return EMPTY_OHLCV_DF

    symbol_norm = symbol.replace("/", "_")  # 规范化交易对名称以便兼容文件系统
    parent_dir = Path(CACHE_DIR / mode / symbol_norm / timeframe)
    parent_dir.mkdir(parents=True, exist_ok=True)

    original_since = since  # 记录原始请求的 since
    original_limit = limit  # 记录原始请求的 limit

    # 如果未提供 since，则根据 limit 确定默认值
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
                f"警告: 无效或非正数 limit '{limit}' 且 since=None。计算默认 {default_limit} 根 K 线的 since。"
            )
            since = now_ms - (default_limit * tf_ms)
            since = (since // tf_ms) * tf_ms

    print(
        f"处理缓存 (最终): {symbol}, {timeframe}, since={since}, limit={original_limit}"
    )

    # --- 查找缓存文件 ---
    cache_files = sorted(parent_dir.glob(f"{symbol_norm}_{timeframe}_*.csv"))
    valid_files = []
    if cache_files and since is not None:
        for f in cache_files:
            try:
                parts = f.stem.split("_")
                if len(parts) >= 3:
                    file_date_str = parts[-1]
                    file_since_dt = datetime.strptime(
                        file_date_str, "%Y%m%d %H%M%S"
                    ).replace(tzinfo=timezone.utc)
                    file_since_ms = int(file_since_dt.timestamp() * 1000)

                    if file_since_ms <= since:
                        valid_files.append((f, file_since_ms))
                    else:
                        print(f"跳过名称格式不符合预期的缓存文件: {f.name}")
                else:
                    print(f"跳过名称格式不符合预期的缓存文件: {f.name}")
            except ValueError:
                print(f"无法从缓存文件名解析日期: {f.name}。跳过。")
            except Exception as e:
                print(f"处理缓存文件 {f.name} 时出错: {e}。跳过")

        print(
            f"找到 {len(cache_files)} 个缓存文件。找到 {len(valid_files)} 个可能相关的有效文件 (开始时间 <= {since})。"
        )

    # --- 基于缓存可用性的逻辑 ---
    latest_cache_path = None
    df_processed = EMPTY_OHLCV_DF  # 用于存储最终要返回的数据
    cache_used = False  # 标记是否使用了缓存数据

    if valid_files:
        latest_cache_path, cache_start_ms = max(valid_files, key=lambda x: x[1])
        print(f"选择最新的有效缓存: {latest_cache_path} (文件开始于 {cache_start_ms})")

        df_cache = read_ohlcv_from_file(latest_cache_path)

        if not df_cache.empty:
            cache_min_ts = df_cache["timestamp"].min()
            cache_max_ts = df_cache["timestamp"].max()
            print(f"缓存数据实际范围: {cache_min_ts} 到 {cache_max_ts}")

            if since >= cache_min_ts and since <= cache_max_ts:
                print("请求的开始时间 'since' 在缓存时间范围内。")
                cache_used = True
                # 从缓存中提取所需数据
                df_from_cache = df_cache[df_cache["timestamp"] >= since].sort_values(
                    "timestamp"
                )

                if (
                    isinstance(original_limit, int)
                    and len(df_from_cache) >= original_limit
                ):
                    print(f"从缓存返回 {original_limit} 行数据，满足请求。")
                    df_processed = df_from_cache.head(original_limit).copy()
                elif (
                    isinstance(original_limit, int)
                    and len(df_from_cache) < original_limit
                ):
                    print(
                        f"缓存中符合条件的数据不足 {original_limit} 行 ({len(df_from_cache)} 行)。需要获取更多数据。"
                    )
                    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                    last_cache_ts = df_from_cache["timestamp"].max()
                    fetch_since = last_cache_ts + tf_ms
                    remaining_needed = original_limit - len(df_from_cache)
                    print(f"需要从 {fetch_since} 开始获取 {remaining_needed} 根 K 线。")

                    if remaining_needed > 0:
                        df_new = await get_ohlcv_data(
                            exchange,
                            symbol,
                            timeframe,
                            fetch_since,
                            remaining_needed,
                            done=True,  # 始终为 True
                        )
                        if not df_new.empty:
                            print(f"成功获取了 {len(df_new)} 根新的 K 线。")
                            df_combined = pd.concat(
                                [df_cache, df_new], ignore_index=True
                            )
                            df_combined = df_combined.drop_duplicates(
                                subset="timestamp", keep="first"
                            ).sort_values("timestamp")
                            save_ohlcv_to_file(latest_cache_path, df_combined)
                            print(f"缓存文件 {latest_cache_path} 已更新。")
                            df_processed = (
                                df_combined[df_combined["timestamp"] >= since]
                                .sort_values("timestamp")
                                .head(original_limit)
                                .copy()
                            )
                            print(f"处理后，返回 {len(df_processed)} 行数据。")
                        else:
                            print("未获取到新数据，返回已有的缓存数据。")
                            df_processed = df_from_cache.head(original_limit).copy()
                    else:
                        print("不需要获取更多数据，返回已有的缓存数据。")
                        df_processed = df_from_cache.head(original_limit).copy()

            elif since > cache_max_ts:
                gap_ms = since - cache_max_ts
                print(
                    f"请求的 'since' ({since}) 在缓存结束 ({cache_max_ts}) 之后，存在 {gap_ms / tf_ms:.2f} 个时间周期的间隙。"
                )
                # 获取从缓存结束到请求开始的新数据
                fetch_since = cache_max_ts + tf_ms
                fetch_limit = original_limit  # 假设需要获取 limit 根
                print(f"尝试获取从 {fetch_since} 开始的新数据，最多 {fetch_limit} 根。")
                df_new = await get_ohlcv_data(
                    exchange,
                    symbol,
                    timeframe,
                    fetch_since,
                    fetch_limit,
                    done=True,  # 始终为 True
                )
                if not df_new.empty:
                    print(f"成功获取了 {len(df_new)} 根新的 K 线，填补间隙。")
                    df_combined = pd.concat([df_cache, df_new], ignore_index=True)
                    df_combined = df_combined.drop_duplicates(
                        subset="timestamp", keep="first"
                    ).sort_values("timestamp")
                    save_ohlcv_to_file(latest_cache_path, df_combined)
                    print(f"缓存文件 {latest_cache_path} 已更新，填补了间隙。")
                    df_processed = (
                        df_combined[df_combined["timestamp"] >= since]
                        .head(original_limit)
                        .copy()
                    )
                else:
                    print("获取填补间隙的新数据失败，返回已有的缓存数据。")
                    df_processed = df_cache.head(original_limit).copy()

            elif since < cache_min_ts:
                print(
                    f"请求的 'since' ({since}) 早于缓存开始时间 ({cache_min_ts})。将不使用此缓存，获取全新数据。"
                )
                pass

        else:  # df_cache 为空
            print(f"选择的缓存文件 {latest_cache_path} 为空或读取失败。")
            pass

    # --- Fallback: 未找到有效缓存 / 缓存无效 / 存在大间隙 / 请求早于缓存 ---
    if not cache_used or df_processed.empty:
        print(
            f"执行 Fallback 逻辑：获取全新数据。请求 since={since}, limit={original_limit}"
        )
        df_new = await get_ohlcv_data(
            exchange,
            symbol,
            timeframe,
            since,
            original_limit,
            done=True,  # 始终为 True
        )

        if not df_new.empty:
            new_cache_start_time = df_new["timestamp"].min()
            new_cache_start_dt = datetime.fromtimestamp(
                new_cache_start_time / 1000, timezone.utc
            )
            new_date_str = new_cache_start_dt.strftime("%Y%m%d %H%M%S")
            new_cache_file = (
                parent_dir / f"{symbol_norm}_{timeframe}_{new_date_str}.csv"
            )

            if latest_cache_path and new_cache_file == latest_cache_path:
                print(f"警告：新获取的数据将覆盖之前的缓存文件 {latest_cache_path}。")
                save_ohlcv_to_file(new_cache_file, df_new)
            elif new_cache_file.exists():
                print(
                    f"警告：根据新数据计算出的缓存文件 {new_cache_file} 已存在。尝试合并去重。"
                )
                existing_df = read_ohlcv_from_file(new_cache_file)
                combined_df = pd.concat([existing_df, df_new], ignore_index=True)
                combined_df = combined_df.drop_duplicates(
                    subset="timestamp", keep="first"
                ).sort_values("timestamp")
                save_ohlcv_to_file(new_cache_file, combined_df)
            else:
                print(f"将新获取的数据保存到新缓存文件: {new_cache_file}")
                save_ohlcv_to_file(new_cache_file, df_new)

            df_processed = df_new.copy()
        else:
            print("Fallback 获取新数据也失败或返回空，返回空 DataFrame。")
            df_processed = EMPTY_OHLCV_DF

    # 最终返回
    if not df_processed.empty:
        df_processed = df_processed.head(original_limit).copy()

    print(f"===> 最终返回 {len(df_processed)} 行数据 (来自缓存，始终为已完成)。")
    return df_processed


# --- 合并数据,包含最新的未完成 K 线 ---
async def merge_ohlcv_latest(
    mode: str,
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since: Optional[int],
    limit: int,
    done: bool,  # 接收路由中的 done 参数
) -> pd.DataFrame:
    print(
        f"处理缓存并附加最新 K 线: {symbol}, {timeframe}, since={since}, limit={limit}, done={done}"
    )
    # 先获取缓存中的已完成 K 线
    cached_df = await handle_ohlcv_cache(
        mode, exchange, symbol, timeframe, since, limit
    )

    if cached_df.empty:
        raise Exception(
            "尝试合并最新 K 线时，缓存数据为空。请检查缓存配置和数据获取流程。"
        )

    if done:
        print("请求已完成的 K 线，直接返回缓存数据。")
        return cached_df

    if len(cached_df) >= limit:
        print(f"缓存数据已达到或超过请求的 limit ({limit})，无需额外请求。")
        return cached_df

    last_cached_timestamp = int(cached_df["timestamp"].iloc[-1])
    fetch_since = last_cached_timestamp  # 移除多余的 timeframe 增加
    print(
        f"请求包含未完成的最新 K 线，尝试从交易所获取最近的 2 根 K 线，起始时间戳: {fetch_since}"
    )
    latest_df = await get_ohlcv_data(
        exchange, symbol, timeframe, since=fetch_since, limit=2, done=False
    )

    if not latest_df.empty:
        print(
            f"成功获取到最新的 {len(latest_df)} 根 K 线，最后一条时间戳: {latest_df['timestamp'].iloc[-1]}"
        )
        initial_cached_length = len(cached_df)
        # 合并缓存数据和最新的 K 线，去除重复项（以最新的为准）
        combined_df = pd.concat([cached_df, latest_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(
            subset=["timestamp"], keep="last"
        ).sort_values("timestamp")
        final_length = len(combined_df)
        added_count = final_length - initial_cached_length
        if added_count > 0:
            print(f"成功获取并合并了 {added_count} 根新的未完成或更新的 K 线。")
        else:
            print("未获取到新的未完成或更新的 K 线。")
        return combined_df.head(limit)  # 返回限制数量的数据
    else:
        print("获取最新的 K 线失败，返回缓存数据。")
        return cached_df.head(limit)  # 返回限制数量的数据
