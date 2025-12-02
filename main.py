import os
import time
import json
import requests
import threading
import logging
from collections import OrderedDict
from dotenv import load_dotenv
from web3 import Web3
from datetime import datetime
from functools import wraps

# 加载 .env 环境变量
load_dotenv()

# ================= 配置加载类 =================
class Config:
    """集中管理所有配置项"""
    # RPC 配置
    RPC_URL = os.getenv("RPC_URL", "https://rpc.ankr.com/eth")
    RPC_TIMEOUT = int(os.getenv("RPC_TIMEOUT", 30))
    
    # Chainbase 配置
    CHAINBASE_KEY = os.getenv("CHAINBASE_API_KEY")
    
    # 监控目标配置
    TARGET_TOKEN = os.getenv("TARGET_TOKEN_ADDRESS", "0x6982508145454Ce325dDbE47a25d4ec3d2311933")
    TOP_N = int(os.getenv("TOP_N_HOLDERS", 50))
    THRESHOLD_USD = float(os.getenv("ALERT_THRESHOLD_USD", 10000))
    
    # 轮询配置
    BLOCK_POLL_INTERVAL = int(os.getenv("BLOCK_POLL_INTERVAL", 12))  # 以太坊出块时间
    WHALE_UPDATE_INTERVAL = int(os.getenv("WHALE_UPDATE_INTERVAL", 1800))  # 30分钟更新名单
    PRICE_UPDATE_INTERVAL = int(os.getenv("PRICE_UPDATE_INTERVAL", 60))  # 60秒更新价格
    
    # Telegram 配置
    TG_TOKEN = os.getenv("TG_BOT_TOKEN")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID")
    
    # 重试配置
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", 5))
    BASE_RETRY_DELAY = float(os.getenv("BASE_RETRY_DELAY", 1.0))
    
    # 缓存配置
    TX_CACHE_SIZE = int(os.getenv("TX_CACHE_SIZE", 10000))  # 已处理交易缓存大小
    
    # 忽略名单 (黑洞地址、零地址)
    IGNORE_LIST = {
        "0x0000000000000000000000000000000000000000",
        "0x000000000000000000000000000000000000dEaD"
    }
    
    # 零地址 (用于识别 Mint/Burn)
    ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
    DEAD_ADDRESS = "0x000000000000000000000000000000000000dEaD"


# ================= 日志配置 =================
def setup_logging():
    """配置日志系统，支持文件和控制台输出"""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    log_file = os.getenv("LOG_FILE", "whale_monitor.log")
    
    # 创建格式化器
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # 根日志器
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, log_level))
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件处理器 (可选)
    if log_file:
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logging.getLogger(__name__)

logger = setup_logging()


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


# ================= 核心监控类 =================
class WhaleMonitor:
    """
    ERC20 大户监控器
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
        
        # 核心数据结构
        self.whitelist = set()              # 监控的大户地址集合
        self.whale_details = {}             # 大户详细信息 {addr: {"rank": N, "balance": X}}
        self.token_meta = {"symbol": "UNKNOWN", "decimals": 18}
        self.current_price = 0.0
        
        # 已处理交易缓存 (防重复)
        self.processed_txs = LRUCache(Config.TX_CACHE_SIZE)
        
        # 状态标志
        self._running = False
        self._chainbase_degraded = False    # Chainbase 降级标志
        self._last_whale_update = 0         # 上次名单更新时间
        self._last_price_update = 0         # 上次价格更新时间
        
        # 统计计数器
        self.stats = {
            "blocks_processed": 0,
            "transfers_detected": 0,
            "alerts_sent": 0,
            "errors": 0
        }
        
        # 验证 RPC 连接
        self._verify_connection()
        
        # 初始化 Token 元数据
        self.target_token = self.w3.to_checksum_address(Config.TARGET_TOKEN)
        self._init_token_metadata()
    
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
    
    @with_retry(max_retries=3, exceptions=(Exception,))
    def _init_token_metadata(self):
        """获取 Token 的 Symbol 和 Decimals"""
        abi = [
            {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "type": "function"},
            {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "type": "function"}
        ]
        contract = self.w3.eth.contract(address=self.target_token, abi=abi)
        self.token_meta['symbol'] = contract.functions.symbol().call()
        self.token_meta['decimals'] = contract.functions.decimals().call()
        logger.info(f"🎯 监控目标: {self.token_meta['symbol']} ({self.target_token[:10]}...)")
        logger.info(f"   Decimals: {self.token_meta['decimals']} | 阈值: ${Config.THRESHOLD_USD:,.0f}")

    # ----------------- 模块 A: 巨鲸发现 (Chainbase) -----------------
    def update_whales_via_chainbase(self):
        """通过 Chainbase SQL API 获取持仓排名"""
        if not Config.CHAINBASE_KEY:
            logger.warning("⚠️ 未配置 Chainbase Key，使用内置模拟大户名单进行演示...")
            self._load_mock_whales()
            return True
        
        if self._chainbase_degraded:
            logger.warning("⚠️ Chainbase 处于降级模式，跳过更新，继续使用旧名单")
            return False
        
        logger.info("🔄 正在从 Chainbase 更新 Top Holders 名单...")
        
        query = f"""
        SELECT address, original_amount 
        FROM ethereum.token_holders 
        WHERE token_address = '{Config.TARGET_TOKEN.lower()}' 
        ORDER BY original_amount DESC 
        LIMIT {Config.TOP_N + 10}
        """
        
        url = "https://api.chainbase.online/v1/dw/query"
        headers = {"x-api-key": Config.CHAINBASE_KEY, "Content-Type": "application/json"}
        
        try:
            resp = self._request_with_retry(
                "POST", url, headers=headers, json={"query": query}, timeout=30
            )
            
            if resp.status_code == 429:
                # 额度耗尽，进入降级模式
                self._enter_degraded_mode("API 额度耗尽 (429)")
                return False
            
            if resp.status_code != 200:
                raise Exception(f"API Error: {resp.status_code} - {resp.text[:200]}")
            
            data = resp.json().get('data', {}).get('result', [])
            if not data:
                logger.warning("Chainbase 返回空数据，保留现有名单")
                return False
            
            new_list = []
            rank = 1
            for row in data:
                addr = self.w3.to_checksum_address(row['address'])
                if addr in Config.IGNORE_LIST:
                    continue
                if rank > Config.TOP_N:
                    break
                balance = float(row.get('original_amount', 0))
                new_list.append((addr, rank, balance))
                rank += 1
            
            self._update_local_list(new_list)
            self._last_whale_update = time.time()
            logger.info(f"✅ 名单更新完成 | 监控 {len(self.whitelist)} 个地址")
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Chainbase 网络错误: {e}")
            self.stats["errors"] += 1
            return False
        except Exception as e:
            logger.error(f"Chainbase 更新失败: {e}")
            self.stats["errors"] += 1
            return False
    
    def _load_mock_whales(self):
        """加载模拟大户名单 (用于演示/测试)"""
        mock_whales = [
            ("0xF977814e90dA44bFA03b6295A0616a897441aceC", 1, 0),  # Binance Hot Wallet
            ("0x5a52E96BAcdaBb82fd05763E25335261B270Efcb", 2, 0),
        ]
        self._update_local_list(mock_whales)
    
    def _update_local_list(self, address_rank_balance_tuples):
        """更新内存中的白名单"""
        temp_whitelist = set()
        temp_details = {}
        for item in address_rank_balance_tuples:
            addr, rank = item[0], item[1]
            balance = item[2] if len(item) > 2 else 0
            temp_whitelist.add(addr)
            temp_details[addr] = {"rank": rank, "balance": balance}
        
        # 原子更新
        self.whitelist = temp_whitelist
        self.whale_details = temp_details
    
    def _enter_degraded_mode(self, reason: str):
        """进入降级模式"""
        self._chainbase_degraded = True
        msg = f"⚠️ 系统降级警告\n原因: {reason}\n当前名单将继续使用，但不再更新"
        logger.warning(msg)
        self.send_telegram(msg, is_system=True)
    
    # ----------------- 模块 B: 价格获取 (DeFiLlama) -----------------
    def update_price(self):
        """从 DeFiLlama 获取 Token 价格 (免费且无需 Key)"""
        url = f"https://coins.llama.fi/prices/current/ethereum:{Config.TARGET_TOKEN}"
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            key = f"ethereum:{Config.TARGET_TOKEN}"
            if key in data.get('coins', {}):
                new_price = data['coins'][key]['price']
                if new_price != self.current_price:
                    self.current_price = new_price
                    logger.debug(f"💲 价格更新: ${self.current_price:.8f}")
                self._last_price_update = time.time()
                return True
        except Exception as e:
            logger.warning(f"价格获取失败: {e}")
            self.stats["errors"] += 1
        return False
    
    # ----------------- 模块 C: 实时监听 (RPC) -----------------
    def process_logs(self, logs):
        """处理 Transfer 事件日志"""
        for log in logs:
            try:
                if len(log['topics']) < 3:
                    continue
                
                tx_hash = log['transactionHash'].hex()
                
                # 防重复处理
                if self.processed_txs.contains(tx_hash):
                    continue
                
                # 解析地址 (Log 中的地址是 32 字节，需切片取后 20 字节)
                from_addr = self.w3.to_checksum_address("0x" + log['topics'][1].hex()[-40:])
                to_addr = self.w3.to_checksum_address("0x" + log['topics'][2].hex()[-40:])
                
                # 识别 Mint/Burn 事件
                is_mint = from_addr == Config.ZERO_ADDRESS
                is_burn = to_addr == Config.ZERO_ADDRESS or to_addr == Config.DEAD_ADDRESS
                
                hit_addr = None
                action = ""
                rank = 0
                event_type = "transfer"
                
                # 核心匹配逻辑
                if from_addr in self.whitelist:
                    hit_addr = from_addr
                    rank = self.whale_details[from_addr]['rank']
                    if is_burn:
                        action = "🔥 销毁 (Burn)"
                        event_type = "burn"
                    else:
                        action = "🔴 减持 (Sell/Out)"
                        event_type = "sell"
                elif to_addr in self.whitelist:
                    hit_addr = to_addr
                    rank = self.whale_details[to_addr]['rank']
                    if is_mint:
                        action = "🆕 铸造接收 (Mint)"
                        event_type = "mint"
                    else:
                        action = "🟢 增持 (Buy/In)"
                        event_type = "buy"
                
                if hit_addr:
                    self.stats["transfers_detected"] += 1
                    self.trigger_alert(hit_addr, rank, action, log, event_type)
                    self.processed_txs.add(tx_hash)
                    
            except Exception as e:
                logger.error(f"处理 Log 异常: {e}")
                self.stats["errors"] += 1
    
    def trigger_alert(self, whale_addr, rank, action, log, event_type):
        """触发警报"""
        try:
            # 1. 计算数量
            raw_val = int(log['data'].hex(), 16)
            amount = raw_val / (10 ** self.token_meta['decimals'])
            
            # 2. 计算价值
            usd_value = amount * self.current_price
            
            # 3. 阈值过滤
            if usd_value < Config.THRESHOLD_USD:
                return
            
            # 4. 获取交易详情
            tx_hash = log['transactionHash'].hex()
            block_num = log['blockNumber']
            
            # 5. 生成消息
            msg = self._format_alert_message(
                whale_addr, rank, action, amount, usd_value, 
                tx_hash, block_num, event_type
            )
            
            logger.info(f"\n{'='*50}\n{msg}\n{'='*50}")
            self.send_telegram(msg)
            self.stats["alerts_sent"] += 1
            
        except Exception as e:
            logger.error(f"触发警报异常: {e}")
            self.stats["errors"] += 1
    
    def _format_alert_message(self, whale_addr, rank, action, amount, usd_value, 
                               tx_hash, block_num, event_type):
        """格式化警报消息"""
        # 根据事件类型选择 emoji
        emoji_map = {
            "buy": "🟢",
            "sell": "🔴", 
            "mint": "🆕",
            "burn": "🔥"
        }
        header_emoji = emoji_map.get(event_type, "🚨")
        
        msg = (
            f"{header_emoji} *Whale Alert (Rank #{rank})*\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"*Action:* {action}\n"
            f"*Token:* `{amount:,.0f}` {self.token_meta['symbol']}\n"
            f"*Value:* `${usd_value:,.2f}`\n"
            f"*Address:* `{whale_addr[:8]}...{whale_addr[-6:]}`\n"
            f"*Price:* `${self.current_price:.8f}`\n"
            f"*Block:* `{block_num}`\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"[📊 Etherscan](https://etherscan.io/tx/{tx_hash}) | "
            f"[👤 Address](https://etherscan.io/address/{whale_addr})"
        )
        return msg
    
    def send_telegram(self, text, is_system=False):
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
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code != 200:
                logger.warning(f"Telegram 发送失败: {resp.text[:100]}")
                return False
            return True
        except Exception as e:
            logger.error(f"Telegram 推送异常: {e}")
            return False
    
    # ----------------- 辅助方法 -----------------
    @with_retry(max_retries=3, exceptions=(requests.exceptions.RequestException,))
    def _request_with_retry(self, method, url, **kwargs):
        """带重试的 HTTP 请求"""
        return requests.request(method, url, **kwargs)
    
    def get_status(self):
        """获取监控状态"""
        return {
            "running": self._running,
            "degraded": self._chainbase_degraded,
            "whitelist_size": len(self.whitelist),
            "current_price": self.current_price,
            "tx_cache_size": len(self.processed_txs),
            "stats": self.stats.copy(),
            "last_whale_update": datetime.fromtimestamp(self._last_whale_update).isoformat() if self._last_whale_update else None,
            "last_price_update": datetime.fromtimestamp(self._last_price_update).isoformat() if self._last_price_update else None
        }
    
    def print_status(self):
        """打印状态摘要"""
        status = self.get_status()
        logger.info(
            f"📊 状态 | 监控: {status['whitelist_size']} 地址 | "
            f"价格: ${status['current_price']:.8f} | "
            f"警报: {status['stats']['alerts_sent']} | "
            f"错误: {status['stats']['errors']}"
        )
    
    # ----------------- 启动逻辑 -----------------
    def start(self):
        """启动监控系统"""
        logger.info("🚀 监控系统启动中...")
        self._running = True
        
        # 发送启动通知
        startup_msg = (
            f"🚀 *Whale Monitor Started*\n"
            f"Token: `{self.token_meta['symbol']}`\n"
            f"Address: `{self.target_token[:10]}...`\n"
            f"Threshold: `${Config.THRESHOLD_USD:,.0f}`\n"
            f"Top N: `{Config.TOP_N}`"
        )
        self.send_telegram(startup_msg, is_system=True)
        
        # 1. 启动后台线程: 定期更新名单和价格
        def background_updater():
            while self._running:
                try:
                    now = time.time()
                    
                    # 更新大户名单
                    if now - self._last_whale_update >= Config.WHALE_UPDATE_INTERVAL:
                        self.update_whales_via_chainbase()
                    
                    # 更新价格
                    if now - self._last_price_update >= Config.PRICE_UPDATE_INTERVAL:
                        self.update_price()
                    
                    time.sleep(10)  # 检查间隔
                    
                except Exception as e:
                    logger.error(f"后台更新异常: {e}")
                    self.stats["errors"] += 1
                    time.sleep(30)
        
        # 2. 启动状态打印线程
        def status_printer():
            while self._running:
                time.sleep(300)  # 每 5 分钟打印一次状态
                if self._running:
                    self.print_status()
        
        # 启动后台线程
        updater_thread = threading.Thread(target=background_updater, daemon=True, name="Updater")
        status_thread = threading.Thread(target=status_printer, daemon=True, name="StatusPrinter")
        updater_thread.start()
        status_thread.start()
        
        # 初始化数据
        logger.info("⏳ 正在初始化数据...")
        self.update_whales_via_chainbase()
        self.update_price()
        
        # 等待数据就绪
        time.sleep(2)
        
        if not self.whitelist:
            logger.error("❌ 名单为空，无法启动监控")
            return
        
        if self.current_price <= 0:
            logger.warning("⚠️ 价格获取失败，将使用 0 价格 (可能导致所有交易被过滤)")
        
        # 3. 主循环: 实时监听 RPC
        latest_block = self.w3.eth.block_number
        logger.info(f"📡 开始监听链上 Transfer 事件 (Block #{latest_block})...")
        
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while self._running:
            try:
                current_block = self.w3.eth.block_number
                
                if current_block > latest_block:
                    # 获取日志
                    logs = self.w3.eth.get_logs({
                        'fromBlock': latest_block + 1,
                        'toBlock': current_block,
                        'address': self.target_token,
                        'topics': [self.TRANSFER_TOPIC]
                    })
                    
                    if logs:
                        self.process_logs(logs)
                    
                    self.stats["blocks_processed"] += (current_block - latest_block)
                    latest_block = current_block
                    consecutive_errors = 0  # 重置错误计数
                
                time.sleep(Config.BLOCK_POLL_INTERVAL)
                
            except KeyboardInterrupt:
                logger.info("⏹️ 收到中断信号，正在停止...")
                self.stop()
                break
            except Exception as e:
                consecutive_errors += 1
                self.stats["errors"] += 1
                
                if consecutive_errors >= max_consecutive_errors:
                    error_msg = f"❌ 连续错误达到 {max_consecutive_errors} 次，系统暂停"
                    logger.error(error_msg)
                    self.send_telegram(error_msg, is_system=True)
                    time.sleep(60)  # 暂停 1 分钟
                    consecutive_errors = 0
                else:
                    delay = min(5 * consecutive_errors, 30)
                    logger.error(f"主循环异常 ({consecutive_errors}/{max_consecutive_errors}): {e}, {delay}s 后重试")
                    time.sleep(delay)
    
    def stop(self):
        """停止监控系统"""
        self._running = False
        logger.info("🛑 监控系统已停止")
        self.print_status()
        
        # 发送停止通知
        stop_msg = (
            f"🛑 *Whale Monitor Stopped*\n"
            f"Blocks: `{self.stats['blocks_processed']}`\n"
            f"Alerts: `{self.stats['alerts_sent']}`\n"
            f"Errors: `{self.stats['errors']}`"
        )
        self.send_telegram(stop_msg, is_system=True)


# ================= 入口点 =================
if __name__ == "__main__":
    try:
        monitor = WhaleMonitor()
        monitor.start()
    except KeyboardInterrupt:
        logger.info("程序被用户中断")
    except Exception as e:
        logger.exception(f"程序异常退出: {e}")
        raise
