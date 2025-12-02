"""
ERC20 Whale Monitor - 大户监控主程序 (多 Token 版本)

功能:
- 支持批量监控多个 ERC20 Token
- 定期从 Chainbase 获取 Top Holders 名单
- 实时监听链上 Transfer 事件 (批量解析优化)
- 触发阈值后推送 Telegram 通知
- 本地缓存支持，API 失败时自动回退
"""

import time
import requests
import threading
from collections import OrderedDict
from web3 import Web3
from datetime import datetime
from functools import wraps
from typing import Dict, Set, List, Tuple, Optional

# 导入配置和缓存
from config import Config, setup_logging
from cache import get_cache

# 初始化日志
logger = setup_logging()

# 初始化缓存
whale_cache = get_cache(Config.CACHE_DIR)


# ================= LRU 缓存实现 =================
class LRUCache:
    """
    LRU 缓存，用于存储已处理的交易哈希
    防止 RPC 节点重组或重复推送导致的消息重复发送
    """
    def __init__(self, capacity: int):
        self.cache = OrderedDict()
        self.capacity = capacity
        self._lock = threading.Lock()
    
    def contains(self, key: str) -> bool:
        with self._lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return True
            return False
    
    def add(self, key: str):
        with self._lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                self.cache[key] = True
                if len(self.cache) > self.capacity:
                    self.cache.popitem(last=False)
    
    def __len__(self):
        return len(self.cache)


# ================= 重试装饰器 =================
def with_retry(max_retries=None, base_delay=None, exceptions=(Exception,)):
    """
    指数退避重试装饰器
    失败后等待 base_delay * 2^attempt 秒后重试
    """
    if max_retries is None:
        max_retries = Config.MAX_RETRIES
    if base_delay is None:
        base_delay = Config.BASE_RETRY_DELAY
    
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"[{func.__name__}] 第 {attempt + 1} 次失败: {e}, {delay:.1f}s 后重试...")
                        time.sleep(delay)
                    else:
                        logger.error(f"[{func.__name__}] 达到最大重试次数 ({max_retries}), 最后错误: {e}")
            raise last_exception
        return wrapper
    return decorator


# ================= Token 数据结构 =================
class TokenInfo:
    """单个 Token 的监控数据"""
    def __init__(self, address: str, top_n: int, threshold_usd: float):
        self.address = address
        self.top_n = top_n
        self.threshold_usd = threshold_usd
        self.symbol = "UNKNOWN"
        self.decimals = 18
        self.price = 0.0
        self.whitelist: Set[str] = set()
        self.whale_details: Dict[str, dict] = {}
        self.last_whale_update = 0
        self.last_price_update = 0
        self.chainbase_degraded = False


# ================= 核心监控类 =================
class MultiTokenWhaleMonitor:
    """
    多 Token ERC20 大户监控器
    - 支持同时监控多个 ERC20 Token
    - 批量获取日志，优化解析效率
    - 定期从 Chainbase 获取 Top Holders 名单
    - 实时监听链上 Transfer 事件
    - 触发阈值后推送 Telegram 通知
    """
    
    # ERC20 Transfer 事件签名
    TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    
    def __init__(self):
        self.w3 = Web3(Web3.HTTPProvider(
            Config.RPC_URL, 
            request_kwargs={'timeout': Config.RPC_TIMEOUT}
        ))
        
        # 多 Token 数据结构
        self.tokens: Dict[str, TokenInfo] = {}  # {checksum_address: TokenInfo}
        self.address_to_checksum: Dict[str, str] = {}  # {lower_address: checksum_address}
        
        # 全局大户地址索引 (用于快速匹配)
        # {whale_address: {token_address: rank}}
        self.global_whale_index: Dict[str, Dict[str, int]] = {}
        
        # 已处理交易缓存 (防重复)
        self.processed_txs = LRUCache(Config.TX_CACHE_SIZE)
        
        # 状态标志
        self._running = False
        self._index_lock = threading.Lock()  # 保护全局索引
        
        # 统计计数器
        self.stats = {
            "blocks_processed": 0,
            "transfers_detected": 0,
            "alerts_sent": 0,
            "errors": 0
        }
        
        # 验证 RPC 连接
        self._verify_connection()
        
        # 初始化所有 Token
        self._init_tokens()
    
    def _verify_connection(self):
        """验证 RPC 连接"""
        try:
            if self.w3.is_connected():
                chain_id = self.w3.eth.chain_id
                block_num = self.w3.eth.block_number
                logger.info(f"✅ RPC 连接成功 | Chain ID: {chain_id} | 当前区块: {block_num}")
            else:
                raise ConnectionError("RPC 连接失败")
        except Exception as e:
            logger.error(f"❌ RPC 连接失败: {e}")
            logger.error("请检查 .env 中的 RPC_URL 配置")
            raise SystemExit(1)
    
    def _init_tokens(self):
        """初始化所有监控的 Token"""
        target_tokens = Config.get_target_tokens()
        
        if not target_tokens:
            logger.error("❌ 未配置任何监控 Token")
            raise SystemExit(1)
        
        logger.info(f"📋 正在初始化 {len(target_tokens)} 个 Token...")
        
        for address, config in target_tokens.items():
            try:
                checksum_addr = self.w3.to_checksum_address(address)
                token_info = TokenInfo(
                    address=checksum_addr,
                    top_n=config["top_n"],
                    threshold_usd=config["threshold_usd"]
                )
                
                # 获取 Token 元数据
                self._init_token_metadata(token_info)
                
                self.tokens[checksum_addr] = token_info
                self.address_to_checksum[address.lower()] = checksum_addr
                
                logger.info(
                    f"  🎯 {token_info.symbol} ({checksum_addr[:10]}...) | "
                    f"Top {token_info.top_n} | 阈值 ${token_info.threshold_usd:,.0f}"
                )
            except Exception as e:
                logger.error(f"  ❌ 初始化 Token {address} 失败: {e}")
                self.stats["errors"] += 1
        
        if not self.tokens:
            logger.error("❌ 没有成功初始化任何 Token")
            raise SystemExit(1)
        
        logger.info(f"✅ 成功初始化 {len(self.tokens)} 个 Token")
    
    @with_retry(max_retries=3, exceptions=(Exception,))
    def _init_token_metadata(self, token_info: TokenInfo):
        """获取 Token 的 Symbol 和 Decimals"""
        abi = [
            {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
            {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
        ]
        contract = self.w3.eth.contract(address=token_info.address, abi=abi)
        token_info.symbol = contract.functions.symbol().call()
        token_info.decimals = contract.functions.decimals().call()

    # ----------------- 模块 A: 巨鲸发现 (Chainbase / Ethplorer / Cache) -----------------
    def update_all_whales(self):
        """更新所有 Token 的大户名单"""
        for token_addr, token_info in self.tokens.items():
            try:
                self._update_whales_for_token(token_info)
            except Exception as e:
                logger.error(f"更新 {token_info.symbol} 大户名单失败: {e}")
                self.stats["errors"] += 1
    
    def _update_whales_for_token(self, token_info: TokenInfo):
        """
        获取单个 Token 的 Top Holders 名单
        优先级: 有效缓存 → Chainbase → Ethplorer → 过期缓存
        """
        # 1. 首先检查本地缓存是否有效 (未过期)
        if Config.CACHE_MAX_AGE:
            cache_info = whale_cache.get_cache_info(token_info.address)
            if cache_info:
                cache_age = time.time() - cache_info.get('updated_at', 0)
                if cache_age < Config.CACHE_MAX_AGE:
                    # 缓存有效，直接使用
                    holders = whale_cache.load_holders(token_info.address)
                    if holders:
                        self._update_token_whitelist(token_info, holders, source="cache", save_cache=False)
                        cache_age_str = self._format_duration(cache_age)
                        logger.info(
                            f"✅ [{token_info.symbol}] 使用有效缓存 | {len(token_info.whitelist)} 地址 | "
                            f"缓存年龄: {cache_age_str}"
                        )
                        return True
        
        # 2. 缓存无效或过期，尝试从 API 获取
        # 尝试从 Chainbase 获取
        if Config.CHAINBASE_KEY and not token_info.chainbase_degraded:
            result = self._fetch_from_chainbase(token_info)
            if result:
                return True
        elif not Config.CHAINBASE_KEY:
            logger.debug(f"⚠️ 未配置 Chainbase Key，尝试其他数据源...")
        
        # 尝试从 Ethplorer 获取
        result = self._fetch_from_ethplorer(token_info)
        if result:
            return True
        
        # 3. API 均失败，尝试使用过期缓存作为备份
        result = self._load_from_cache(token_info)
        if result:
            return True
        
        # 所有数据源均失败
        logger.error(f"❌ {token_info.symbol} 所有数据源均失败")
        return False
    
    def _fetch_from_chainbase(self, token_info: TokenInfo) -> bool:
        """从 Chainbase 获取数据"""
        logger.info(f"🔄 [{token_info.symbol}] 正在从 Chainbase 更新 Top Holders...")
        
        url = f"https://api.chainbase.online/v1/token/top-holders"
        headers = {"x-api-key": Config.CHAINBASE_KEY}
        params = {
            "chain_id": 1,  # Ethereum Mainnet
            "contract_address": token_info.address.lower(),
            "page": 1,
            "limit": token_info.top_n + 10
        }
        
        try:
            resp = self._request_with_retry(
                "GET", url, headers=headers, params=params, timeout=Config.HTTP_TIMEOUT
            )
            
            if resp.status_code == 429:
                self._enter_degraded_mode(token_info, "Chainbase API 额度耗尽 (429)")
                return False
            
            if resp.status_code != 200:
                logger.warning(f"[{token_info.symbol}] Chainbase API 错误: {resp.status_code}")
                return False
            
            result = resp.json()
            data = result.get('data', [])
            
            if not data:
                logger.warning(f"[{token_info.symbol}] Chainbase 返回空数据")
                return False
            
            new_list = []
            rank = 1
            for row in data:
                addr = self.w3.to_checksum_address(row.get('wallet_address', row.get('address', '')))
                if addr in Config.IGNORE_LIST:
                    continue
                if rank > token_info.top_n:
                    break
                balance = float(row.get('original_amount', row.get('amount', 0)))
                new_list.append((addr, rank, balance))
                rank += 1
            
            if new_list:
                self._update_token_whitelist(token_info, new_list, source="chainbase")
                logger.info(f"✅ [{token_info.symbol}] Chainbase 更新完成 | 监控 {len(token_info.whitelist)} 地址")
                return True
            
            return False
            
        except requests.exceptions.RequestException as e:
            logger.error(f"[{token_info.symbol}] Chainbase 网络错误: {e}")
            self.stats["errors"] += 1
            return False
        except Exception as e:
            logger.error(f"[{token_info.symbol}] Chainbase 更新失败: {e}")
            self.stats["errors"] += 1
            return False
    
    def _fetch_from_ethplorer(self, token_info: TokenInfo) -> bool:
        """从 Ethplorer 获取数据"""
        logger.info(f"🔄 [{token_info.symbol}] 正在从 Ethplorer 更新 Top Holders...")
        
        url = f"https://api.ethplorer.io/getTopTokenHolders/{token_info.address}"
        params = {
            "apiKey": "freekey",
            "limit": min(token_info.top_n + 10, 100)
        }
        
        try:
            resp = requests.get(url, params=params, timeout=Config.HTTP_TIMEOUT)
            
            if resp.status_code == 429:
                logger.warning(f"[{token_info.symbol}] Ethplorer API 被限流")
                return False
            
            if resp.status_code != 200:
                logger.warning(f"[{token_info.symbol}] Ethplorer API 错误: {resp.status_code}")
                return False
            
            data = resp.json()
            holders = data.get('holders', [])
            
            if not holders:
                logger.warning(f"[{token_info.symbol}] Ethplorer 返回空数据")
                return False
            
            new_list = []
            rank = 1
            for holder in holders:
                addr = self.w3.to_checksum_address(holder.get('address', ''))
                if addr in Config.IGNORE_LIST:
                    continue
                if rank > token_info.top_n:
                    break
                balance = float(holder.get('balance', 0))
                new_list.append((addr, rank, balance))
                rank += 1
            
            if new_list:
                self._update_token_whitelist(token_info, new_list, source="ethplorer")
                logger.info(f"✅ [{token_info.symbol}] Ethplorer 更新完成 | 监控 {len(token_info.whitelist)} 地址")
                return True
            
            return False
                
        except Exception as e:
            logger.error(f"[{token_info.symbol}] Ethplorer 更新失败: {e}")
            self.stats["errors"] += 1
            return False
    
    def _load_from_cache(self, token_info: TokenInfo) -> bool:
        """从本地缓存加载数据"""
        logger.info(f"🔄 [{token_info.symbol}] 正在从本地缓存加载...")
        
        cache_info = whale_cache.get_cache_info(token_info.address)
        if not cache_info:
            logger.warning(f"[{token_info.symbol}] 本地缓存不存在")
            return False
        
        holders = whale_cache.load_holders(token_info.address)
        if not holders:
            logger.warning(f"[{token_info.symbol}] 本地缓存加载失败")
            return False
        
        self._update_token_whitelist(token_info, holders, source="cache", save_cache=False)
        
        cache_age = time.time() - cache_info.get('updated_at', 0)
        cache_age_str = self._format_duration(cache_age)
        
        logger.info(
            f"✅ [{token_info.symbol}] 从缓存加载完成 | 监控 {len(token_info.whitelist)} 地址 | "
            f"缓存年龄: {cache_age_str}"
        )
        return True
    
    def _update_token_whitelist(
        self, 
        token_info: TokenInfo,
        address_rank_balance_tuples: List[Tuple[str, int, float]], 
        source: str = "unknown",
        save_cache: bool = True
    ):
        """
        更新单个 Token 的白名单，并同步更新全局索引
        """
        # 更新 Token 本地数据
        temp_whitelist = set()
        temp_details = {}
        for item in address_rank_balance_tuples:
            addr, rank = item[0], item[1]
            balance = item[2] if len(item) > 2 else 0
            temp_whitelist.add(addr)
            temp_details[addr] = {"rank": rank, "balance": balance}
        
        old_whitelist = token_info.whitelist
        token_info.whitelist = temp_whitelist
        token_info.whale_details = temp_details
        token_info.last_whale_update = time.time()
        
        # 更新全局索引 (线程安全)
        with self._index_lock:
            # 移除旧地址
            for addr in old_whitelist:
                if addr in self.global_whale_index:
                    self.global_whale_index[addr].pop(token_info.address, None)
                    if not self.global_whale_index[addr]:
                        del self.global_whale_index[addr]
            
            # 添加新地址
            for addr in temp_whitelist:
                if addr not in self.global_whale_index:
                    self.global_whale_index[addr] = {}
                self.global_whale_index[addr][token_info.address] = temp_details[addr]["rank"]
        
        # 保存到本地缓存
        if save_cache and source in ("chainbase", "ethplorer"):
            whale_cache.save(
                token_address=token_info.address,
                holders=list(address_rank_balance_tuples),
                symbol=token_info.symbol,
                source=source,
                decimals=token_info.decimals
            )
            logger.debug(f"💾 [{token_info.symbol}] 已保存到本地缓存")
    
    def _enter_degraded_mode(self, token_info: TokenInfo, reason: str):
        """进入降级模式"""
        token_info.chainbase_degraded = True
        msg = f"⚠️ [{token_info.symbol}] 降级警告\n原因: {reason}"
        logger.warning(msg)
        self.send_telegram(msg, is_system=True)
    
    @staticmethod
    def _format_duration(seconds: float) -> str:
        """格式化时长"""
        if seconds < 60:
            return f"{seconds:.0f}秒"
        elif seconds < 3600:
            return f"{seconds/60:.0f}分钟"
        elif seconds < 86400:
            return f"{seconds/3600:.1f}小时"
        else:
            return f"{seconds/86400:.1f}天"
    
    # ----------------- 模块 B: 价格获取 (DeFiLlama) -----------------
    def update_all_prices(self):
        """批量更新所有 Token 价格 (一次 API 调用)"""
        if not self.tokens:
            return False
        
        # 构建批量查询 URL
        token_keys = [f"ethereum:{addr}" for addr in self.tokens.keys()]
        url = f"https://coins.llama.fi/prices/current/{','.join(token_keys)}"
        
        try:
            resp = requests.get(url, timeout=Config.HTTP_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            coins = data.get('coins', {})
            
            updated_count = 0
            for token_addr, token_info in self.tokens.items():
                key = f"ethereum:{token_addr}"
                if key in coins:
                    new_price = coins[key]['price']
                    if new_price != token_info.price:
                        token_info.price = new_price
                        logger.debug(f"💲 [{token_info.symbol}] 价格: ${new_price:.8f}")
                    token_info.last_price_update = time.time()
                    updated_count += 1
            
            logger.debug(f"💲 批量价格更新完成 | {updated_count}/{len(self.tokens)} 成功")
            return updated_count > 0
            
        except Exception as e:
            logger.warning(f"批量价格获取失败: {e}")
            self.stats["errors"] += 1
            return False
    
    # ----------------- 模块 C: 实时监听 (RPC) - 优化版 -----------------
    def get_batch_logs(self, from_block: int, to_block: int) -> List:
        """
        批量获取所有监控 Token 的 Transfer 日志
        单次 RPC 调用获取所有 Token 的事件
        """
        token_addresses = list(self.tokens.keys())
        
        try:
            logs = self.w3.eth.get_logs({
                'fromBlock': from_block,
                'toBlock': to_block,
                'address': token_addresses,  # 批量查询多个合约
                'topics': [self.TRANSFER_TOPIC]
            })
            return logs
        except Exception as e:
            logger.error(f"批量获取日志失败: {e}")
            self.stats["errors"] += 1
            return []
    
    def process_logs_batch(self, logs: List):
        """
        批量处理 Transfer 事件日志 (优化版)
        - 预计算地址转换
        - 使用全局索引快速匹配
        - 批量处理减少锁竞争
        """
        if not logs:
            return
        
        # 预处理: 按 Token 分组
        alerts_to_send = []
        
        for log in logs:
            try:
                if len(log['topics']) < 3:
                    continue
                
                tx_hash = log['transactionHash'].hex()
                
                # 防重复处理
                if self.processed_txs.contains(tx_hash):
                    continue
                
                # 获取 Token 地址
                log_address = log['address']
                if isinstance(log_address, bytes):
                    log_address = log_address.hex()
                token_addr = self.w3.to_checksum_address(log_address)
                
                if token_addr not in self.tokens:
                    continue
                
                token_info = self.tokens[token_addr]
                
                # 解析地址 (优化: 直接切片，避免重复转换)
                from_addr = self.w3.to_checksum_address("0x" + log['topics'][1].hex()[-40:])
                to_addr = self.w3.to_checksum_address("0x" + log['topics'][2].hex()[-40:])
                
                # 识别 Mint/Burn 事件
                is_mint = from_addr == Config.ZERO_ADDRESS
                is_burn = to_addr == Config.ZERO_ADDRESS or to_addr == Config.DEAD_ADDRESS
                
                # 使用全局索引快速匹配
                hit_addr = None
                action = ""
                rank = 0
                event_type = "transfer"
                
                with self._index_lock:
                    # 检查 from 地址
                    if from_addr in self.global_whale_index:
                        token_ranks = self.global_whale_index[from_addr]
                        if token_addr in token_ranks:
                            hit_addr = from_addr
                            rank = token_ranks[token_addr]
                            if is_burn:
                                action = "🔥 销毁 (Burn)"
                                event_type = "burn"
                            else:
                                action = "🔴 减持 (Sell/Out)"
                                event_type = "sell"
                    
                    # 检查 to 地址
                    if not hit_addr and to_addr in self.global_whale_index:
                        token_ranks = self.global_whale_index[to_addr]
                        if token_addr in token_ranks:
                            hit_addr = to_addr
                            rank = token_ranks[token_addr]
                            if is_mint:
                                action = "🆕 铸造接收 (Mint)"
                                event_type = "mint"
                            else:
                                action = "🟢 增持 (Buy/In)"
                                event_type = "buy"
                
                if hit_addr:
                    self.stats["transfers_detected"] += 1
                    
                    # 计算数量和价值
                    raw_val = int(log['data'].hex(), 16)
                    amount = raw_val / (10 ** token_info.decimals)
                    usd_value = amount * token_info.price
                    
                    # 阈值过滤
                    if usd_value >= token_info.threshold_usd:
                        alerts_to_send.append({
                            "token_info": token_info,
                            "whale_addr": hit_addr,
                            "rank": rank,
                            "action": action,
                            "amount": amount,
                            "usd_value": usd_value,
                            "tx_hash": tx_hash,
                            "block_num": log['blockNumber'],
                            "event_type": event_type
                        })
                    
                    self.processed_txs.add(tx_hash)
                    
            except Exception as e:
                logger.error(f"处理 Log 异常: {e}")
                self.stats["errors"] += 1
        
        # 批量发送警报
        for alert in alerts_to_send:
            self._send_alert(alert)
    
    def _send_alert(self, alert: dict):
        """发送单个警报"""
        try:
            token_info = alert["token_info"]
            msg = self._format_alert_message(
                token_info=token_info,
                whale_addr=alert["whale_addr"],
                rank=alert["rank"],
                action=alert["action"],
                amount=alert["amount"],
                usd_value=alert["usd_value"],
                tx_hash=alert["tx_hash"],
                block_num=alert["block_num"],
                event_type=alert["event_type"]
            )
            
            logger.info(f"\n{'='*50}\n{msg}\n{'='*50}")
            self.send_telegram(msg)
            self.stats["alerts_sent"] += 1
            
        except Exception as e:
            logger.error(f"发送警报异常: {e}")
            self.stats["errors"] += 1
    
    def _format_alert_message(self, token_info: TokenInfo, whale_addr: str, rank: int, 
                               action: str, amount: float, usd_value: float,
                               tx_hash: str, block_num: int, event_type: str) -> str:
        """格式化警报消息"""
        # 根据事件类型选择 emoji 和动作描述
        event_config = {
            "buy": {"emoji": "🟢", "action_text": "增持"},
            "sell": {"emoji": "🔴", "action_text": "减持"},
            "mint": {"emoji": "🆕", "action_text": "铸造"},
            "burn": {"emoji": "🔥", "action_text": "销毁"}
        }
        config = event_config.get(event_type, {"emoji": "🚨", "action_text": "转账"})
        header_emoji = config["emoji"]
        action_text = config["action_text"]
        
        # 格式化价格显示 (根据价格大小动态调整精度)
        if token_info.price >= 1:
            price_str = f"${token_info.price:,.4f}"
        elif token_info.price >= 0.0001:
            price_str = f"${token_info.price:.6f}"
        else:
            price_str = f"${token_info.price:.10f}"
        
        # 格式化数量显示 (根据数量大小动态调整)
        if amount >= 1_000_000_000:
            amount_str = f"{amount/1_000_000_000:,.2f}B"
        elif amount >= 1_000_000:
            amount_str = f"{amount/1_000_000:,.2f}M"
        elif amount >= 1_000:
            amount_str = f"{amount/1_000:,.2f}K"
        else:
            amount_str = f"{amount:,.2f}"
        
        # 格式化价值显示
        if usd_value >= 1_000_000:
            value_str = f"${usd_value/1_000_000:,.2f}M"
        elif usd_value >= 1_000:
            value_str = f"${usd_value/1_000:,.2f}K"
        else:
            value_str = f"${usd_value:,.2f}"
        
        msg = (
            f"{header_emoji} *{token_info.symbol} 大户{action_text}*\n"
            f"┌───────────────────────\n"
            f"│ 🏷️ *排名:* `#{rank}`\n"
            f"│ 💰 *数量:* `{amount_str}` {token_info.symbol}\n"
            f"│ 💵 *价值:* `{value_str}`\n"
            f"│ 👛 *地址:* `{whale_addr[:6]}...{whale_addr[-4:]}`\n"
            f"│ 📈 *价格:* `{price_str}`\n"
            f"└───────────────────────\n"
            f"[🔗 交易详情](https://etherscan.io/tx/{tx_hash}) · "
            f"[📋 地址](https://etherscan.io/address/{whale_addr})"
        )
        return msg
    
    def send_telegram(self, text: str, is_system: bool = False) -> bool:
        """发送 Telegram 消息"""
        if not Config.TG_TOKEN or not Config.TG_CHAT_ID:
            return False
        
        url = f"https://api.telegram.org/bot{Config.TG_TOKEN}/sendMessage"
        payload = {
            "chat_id": Config.TG_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True
        }
        
        try:
            resp = requests.post(url, json=payload, timeout=Config.HTTP_TIMEOUT)
            if resp.status_code != 200:
                logger.warning(f"Telegram 发送失败: {resp.text[:100]}")
                return False
            return True
        except Exception as e:
            logger.error(f"Telegram 推送异常: {e}")
            return False
    
    # ----------------- 辅助方法 -----------------
    @with_retry(max_retries=3, exceptions=(requests.exceptions.RequestException,))
    def _request_with_retry(self, method: str, url: str, **kwargs):
        """带重试的 HTTP 请求"""
        return requests.request(method, url, **kwargs)
    
    def get_status(self) -> dict:
        """获取监控状态"""
        token_status = []
        total_whales = 0
        for addr, info in self.tokens.items():
            total_whales += len(info.whitelist)
            token_status.append({
                "symbol": info.symbol,
                "address": addr[:10] + "...",
                "whitelist_size": len(info.whitelist),
                "price": info.price,
                "degraded": info.chainbase_degraded
            })
        
        return {
            "running": self._running,
            "tokens_count": len(self.tokens),
            "total_whales": total_whales,
            "global_index_size": len(self.global_whale_index),
            "tx_cache_size": len(self.processed_txs),
            "stats": self.stats.copy(),
            "tokens": token_status
        }
    
    def print_status(self):
        """打印状态摘要"""
        status = self.get_status()
        token_summary = " | ".join([
            f"{t['symbol']}:{t['whitelist_size']}" 
            for t in status['tokens']
        ])
        logger.info(
            f"📊 状态 | Token: {status['tokens_count']} | "
            f"大户: {status['total_whales']} | "
            f"索引: {status['global_index_size']} | "
            f"警报: {status['stats']['alerts_sent']} | "
            f"错误: {status['stats']['errors']}"
        )
        logger.debug(f"   详情: {token_summary}")
    
    # ----------------- 启动逻辑 -----------------
    def start(self):
        """启动监控系统"""
        logger.info("🚀 多 Token 监控系统启动中...")
        self._running = True
        
        # 发送启动通知
        token_list = "\n".join([
            f"  • {info.symbol} (Top {info.top_n}, ${info.threshold_usd:,.0f})"
            for info in self.tokens.values()
        ])
        startup_msg = (
            f"🚀 *Multi-Token Whale Monitor Started*\n"
            f"监控 Token 数量: `{len(self.tokens)}`\n"
            f"{token_list}"
        )
        self.send_telegram(startup_msg, is_system=True)
        
        # 1. 启动后台线程: 定期更新名单和价格
        def background_updater():
            # 等待初始化完成后再开始检查更新
            time.sleep(Config.BLOCK_POLL_INTERVAL * 2)
            
            while self._running:
                try:
                    now = time.time()
                    
                    # 检查是否需要更新大户名单
                    for token_info in self.tokens.values():
                        if now - token_info.last_whale_update >= Config.WHALE_UPDATE_INTERVAL:
                            self._update_whales_for_token(token_info)
                    
                    # 批量更新价格
                    min_price_update = min(
                        (t.last_price_update for t in self.tokens.values()),
                        default=0
                    )
                    if now - min_price_update >= Config.PRICE_UPDATE_INTERVAL:
                        self.update_all_prices()
                    
                    time.sleep(10)
                    
                except Exception as e:
                    logger.error(f"后台更新异常: {e}")
                    self.stats["errors"] += 1
                    time.sleep(30)
        
        # 2. 启动状态打印线程
        def status_printer():
            while self._running:
                time.sleep(Config.STATUS_PRINT_INTERVAL)
                if self._running:
                    self.print_status()
        
        # 启动后台线程
        updater_thread = threading.Thread(target=background_updater, daemon=True, name="Updater")
        status_thread = threading.Thread(target=status_printer, daemon=True, name="StatusPrinter")
        updater_thread.start()
        status_thread.start()
        
        # 初始化数据
        logger.info("⏳ 正在初始化数据...")
        self.update_all_whales()
        self.update_all_prices()
        
        # 等待数据就绪
        time.sleep(2)
        
        total_whales = sum(len(t.whitelist) for t in self.tokens.values())
        if total_whales == 0:
            logger.error("❌ 所有 Token 名单均为空，无法启动监控")
            return
        
        # 检查价格
        tokens_without_price = [t.symbol for t in self.tokens.values() if t.price <= 0]
        if tokens_without_price:
            logger.warning(f"⚠️ 以下 Token 价格获取失败: {', '.join(tokens_without_price)}")
        
        # 3. 主循环: 实时监听 RPC (批量获取)
        latest_block = self.w3.eth.block_number
        logger.info(f"📡 开始监听链上 Transfer 事件 (Block #{latest_block})...")
        logger.info(f"   监控 {len(self.tokens)} 个 Token, 共 {total_whales} 个大户地址")
        
        consecutive_errors = 0
        last_activity_time = time.time()
        last_heartbeat_time = time.time()
        poll_count = 0
        
        HEARTBEAT_INTERVAL = 60
        STALE_THRESHOLD = 180
        
        while self._running:
            try:
                poll_count += 1
                current_time = time.time()
                current_block = self.w3.eth.block_number
                
                if current_block > latest_block:
                    blocks_diff = current_block - latest_block
                    
                    # 批量获取所有 Token 的日志
                    logs = self.get_batch_logs(latest_block + 1, current_block)
                    
                    transfer_count = len(logs) if logs else 0
                    if transfer_count > 0:
                        logger.info(
                            f"📦 Block #{latest_block + 1} → #{current_block} | "
                            f"+{blocks_diff} 区块 | {transfer_count} 笔 Transfer"
                        )
                        # 批量处理日志
                        self.process_logs_batch(logs)
                    else:
                        logger.debug(
                            f"📦 Block #{current_block} | +{blocks_diff} 区块 | 无相关 Transfer"
                        )
                    
                    self.stats["blocks_processed"] += blocks_diff
                    latest_block = current_block
                    last_activity_time = current_time
                    consecutive_errors = 0
                
                # 心跳输出
                if current_time - last_heartbeat_time >= HEARTBEAT_INTERVAL:
                    time_since_activity = current_time - last_activity_time
                    total_whales = sum(len(t.whitelist) for t in self.tokens.values())
                    logger.info(
                        f"💓 心跳 | Block #{latest_block} | "
                        f"轮询 #{poll_count} | "
                        f"距上次新区块: {time_since_activity:.0f}s | "
                        f"监控: {len(self.tokens)} Token / {total_whales} 地址"
                    )
                    last_heartbeat_time = current_time
                    
                    if time_since_activity > STALE_THRESHOLD:
                        logger.warning(
                            f"⚠️ 警告: {time_since_activity:.0f}s 未检测到新区块，"
                            f"RPC 可能存在问题"
                        )
                
                time.sleep(Config.BLOCK_POLL_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("⏹️ 收到中断信号，正在停止...")
                self.stop()
                break
            except Exception as e:
                consecutive_errors += 1
                self.stats["errors"] += 1
                
                if consecutive_errors >= Config.MAX_CONSECUTIVE_ERRORS:
                    error_msg = f"❌ 连续错误达到 {Config.MAX_CONSECUTIVE_ERRORS} 次，系统暂停"
                    logger.error(error_msg)
                    self.send_telegram(error_msg, is_system=True)
                    time.sleep(60)
                    consecutive_errors = 0
                else:
                    delay = min(5 * consecutive_errors, 30)
                    logger.error(f"主循环异常 ({consecutive_errors}/{Config.MAX_CONSECUTIVE_ERRORS}): {e}, {delay}s 后重试")
                    time.sleep(delay)
    
    def stop(self):
        """停止监控系统"""
        self._running = False
        logger.info("🛑 监控系统已停止")
        self.print_status()
        
        stop_msg = (
            f"🛑 *Multi-Token Whale Monitor Stopped*\n"
            f"Tokens: `{len(self.tokens)}`\n"
            f"Blocks: `{self.stats['blocks_processed']}`\n"
            f"Alerts: `{self.stats['alerts_sent']}`\n"
            f"Errors: `{self.stats['errors']}`"
        )
        self.send_telegram(stop_msg, is_system=True)


# ================= 入口点 =================
if __name__ == "__main__":
    # 打印配置信息
    Config.print_config()
    Config.validate()
    
    try:
        monitor = MultiTokenWhaleMonitor()
        monitor.start()
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.exception(f"程序异常退出: {e}")
        raise
