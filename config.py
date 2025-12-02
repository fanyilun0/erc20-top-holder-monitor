"""
ERC20 Whale Monitor 配置模块

配置优先级:
1. 环境变量 (.env) - 仅用于敏感信息 (API Keys, Tokens)
2. 本文件中的默认值 - 用于非敏感配置项

使用方式:
    from config import Config
"""

import os
import logging
from dotenv import load_dotenv

# 加载 .env 环境变量 (仅用于密钥)
load_dotenv()


class Config:
    """
    集中管理所有配置项
    
    密钥类配置从环境变量读取 (.env)
    非密钥类配置直接在此文件中定义
    """
    
    # ============================================================
    # 敏感配置 (从 .env 读取)
    # ============================================================
    
    # Chainbase API Key (可选，不配置则使用 Ethplorer 免费 API)
    CHAINBASE_KEY = os.getenv("CHAINBASE_API_KEY")
    
    # Telegram Bot 配置
    TG_TOKEN = os.getenv("TG_BOT_TOKEN")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID")
    
    # ============================================================
    # 多链配置 (支持的链及其 RPC 和 Chain ID)
    # ============================================================
    
    # 支持的链配置: {chain_name: {"chain_id": X, "rpc_url": "...", "explorer": "...", "defi_llama_prefix": "..."}}
    SUPPORTED_CHAINS = {
        "ethereum": {
            "chain_id": 1,
            "rpc_url": os.getenv("ETH_RPC_URL", "https://rpc.ankr.com/eth"),
            "explorer": "https://etherscan.io",
            "defi_llama_prefix": "ethereum",
            "name": "Ethereum",
        },
        "bsc": {
            "chain_id": 56,
            "rpc_url": os.getenv("BSC_RPC_URL", "https://rpc.ankr.com/bsc"),
            "explorer": "https://bscscan.com",
            "defi_llama_prefix": "bsc",
            "name": "BNB Chain",
        },
        "polygon": {
            "chain_id": 137,
            "rpc_url": os.getenv("POLYGON_RPC_URL", "https://rpc.ankr.com/polygon"),
            "explorer": "https://polygonscan.com",
            "defi_llama_prefix": "polygon",
            "name": "Polygon",
        },
        "arbitrum": {
            "chain_id": 42161,
            "rpc_url": os.getenv("ARBITRUM_RPC_URL", "https://rpc.ankr.com/arbitrum"),
            "explorer": "https://arbiscan.io",
            "defi_llama_prefix": "arbitrum",
            "name": "Arbitrum One",
        },
        "base": {
            "chain_id": 8453,
            "rpc_url": os.getenv("BASE_RPC_URL", "https://rpc.ankr.com/base"),
            "explorer": "https://basescan.org",
            "defi_llama_prefix": "base",
            "name": "Base",
        },
    }
    
    # 兼容旧配置: 默认 RPC URL (以太坊)
    RPC_URL = os.getenv("RPC_URL", os.getenv("ETH_RPC_URL", "https://rpc.ankr.com/eth"))
    
    # ============================================================
    # 监控目标配置 (可在此处直接修改)
    # ============================================================
    
    # 要监控的 ERC20 Token 合约地址列表 (支持多 Token 批量监控)
    # 
    # 简化配置格式 (3 种写法):
    #   1. 字符串地址: 使用全局默认配置 (ethereum 链, DEFAULT_TOP_N, DEFAULT_THRESHOLD_USD)
    #   2. 元组 (地址, 链名): 指定链，使用默认 top_n 和 threshold
    #   3. 字典: 完整自定义配置 {"address": "0x...", "chain": "bsc", "top_n": 50, "threshold_usd": 5000}
    #
    # 支持的链: ethereum, bsc, polygon, arbitrum, base
    #
    TARGET_TOKENS = [
        # ========== Ethereum 链 ==========
        # "0x6982508145454Ce325dDbE47a25d4ec3d2311933",  # PEPE - 使用全局默认配置
        # "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
        # "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
        
        # ========== BSC 链 ==========
        # ("0x2170Ed0880ac9A755fd29B2688956BD959F933F8", "bsc"),  # WETH on BSC
        # ("0x55d398326f99059fF775485246999027B3197955", "bsc"),  # USDT on BSC
        ("0x924fa68a0FC644485b8df8AbfA0A41C2e7744444", "bsc"),  # $币安人生 on BSC
        ("0x82Ec31D69b3c289E541b50E30681FD1ACAd24444", "bsc"),  # $哈基米 on BSC
        ("0x44440f83419DE123d7d411187aDb9962db017d03", "bsc"),  # $BNBHolder on BSC
        # ========== 完整自定义配置示例 ==========
        # {"address": "0x...", "chain": "polygon", "top_n": 50, "threshold_usd": 5000},
    ]
    
    # 默认监控前 N 名持仓大户
    DEFAULT_TOP_N = 100
    
    # 默认警报阈值 (USD)，低于此金额的交易将被忽略
    DEFAULT_THRESHOLD_USD = 100.0
    
    # ============================================================
    # 轮询间隔配置 (秒)
    # ============================================================
    
    # 区块轮询间隔 (以太坊约 12 秒出块，设置更短以提高响应速度)
    BLOCK_POLL_INTERVAL = 3
    
    # 大户名单更新间隔 (默认 30 分钟)
    WHALE_UPDATE_INTERVAL = 1800
    
    # 价格更新间隔 (默认 60 秒)
    PRICE_UPDATE_INTERVAL = 60
    
    # ============================================================
    # 网络配置
    # ============================================================
    
    # RPC 请求超时时间 (秒)
    RPC_TIMEOUT = 30
    
    # HTTP 请求超时时间 (秒)
    HTTP_TIMEOUT = 10
    
    # ============================================================
    # 重试配置
    # ============================================================
    
    # 最大重试次数
    MAX_RETRIES = 5
    
    # 基础重试延迟 (秒)，实际延迟 = BASE_RETRY_DELAY * 2^attempt
    BASE_RETRY_DELAY = 1.0
    
    # 主循环最大连续错误次数
    MAX_CONSECUTIVE_ERRORS = 10
    
    # ============================================================
    # 缓存配置
    # ============================================================
    
    # 已处理交易缓存大小 (防止重复推送)
    TX_CACHE_SIZE = 10000
    
    # 本地缓存目录 (存储大户名单)
    CACHE_DIR = "cache"
    
    # 缓存最大有效期 (秒)，超过此时间优先从 API 获取
    # 默认 30 分钟 (1800 秒)
    CACHE_MAX_AGE = 18000
    
    # ============================================================
    # 日志配置
    # ============================================================
    
    # 日志级别: DEBUG, INFO, WARNING, ERROR
    LOG_LEVEL = "INFO"
    
    # 日志文件路径 (设为 None 则只输出到控制台)
    LOG_FILE = "whale_monitor.log"
    
    # 日志格式
    LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s"
    LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    
    # 状态打印间隔 (秒)
    STATUS_PRINT_INTERVAL = 300  # 5 分钟
    
    # ============================================================
    # 地址常量 (不建议修改)
    # ============================================================
    
    # 忽略名单 (黑洞地址、零地址) - 这些地址不会被加入监控名单
    IGNORE_LIST = {
        "0x0000000000000000000000000000000000000000",
        "0x000000000000000000000000000000000000dEaD"
    }
    
    # 零地址 (用于识别 Mint 事件)
    ZERO_ADDRESS = "0x0000000000000000000000000000000000000000"
    
    # 销毁地址 (用于识别 Burn 事件)
    DEAD_ADDRESS = "0x000000000000000000000000000000000000dEaD"
    
    # ============================================================
    # 类方法
    # ============================================================
    
    @classmethod
    def validate(cls) -> bool:
        """
        验证关键配置是否完整
        返回 True 表示配置有效，False 表示有缺失
        """
        warnings = []
        
        if not cls.RPC_URL:
            warnings.append("RPC_URL 未配置，将使用公共节点 (不推荐用于生产)")
        
        if not cls.CHAINBASE_KEY:
            warnings.append("CHAINBASE_API_KEY 未配置，将使用 Ethplorer 免费 API")
        
        if not cls.TG_TOKEN or not cls.TG_CHAT_ID:
            warnings.append("Telegram 未配置，警报将只输出到日志")
        
        if warnings:
            for w in warnings:
                print(f"⚠️ 配置警告: {w}")
            return False
        
        return True
    
    @classmethod
    def get_target_tokens(cls) -> dict:
        """
        获取要监控的 Token 列表
        
        支持 3 种配置格式:
        1. 字符串: "0x..." -> ethereum 链，使用默认配置
        2. 元组: ("0x...", "bsc") -> 指定链，使用默认配置
        3. 字典: {"address": "0x...", "chain": "bsc", "top_n": 50, "threshold_usd": 5000}
        
        Returns:
            dict: {address: {"top_n": N, "threshold_usd": X, "chain": "ethereum"}, ...}
        """
        tokens = {}
        
        for item in cls.TARGET_TOKENS:
            # 解析不同格式的配置
            if isinstance(item, str):
                # 格式1: 纯地址字符串
                addr = item
                chain = "ethereum"
                top_n = cls.DEFAULT_TOP_N
                threshold_usd = cls.DEFAULT_THRESHOLD_USD
            elif isinstance(item, tuple):
                # 格式2: (地址, 链名) 元组
                addr = item[0]
                chain = item[1] if len(item) > 1 else "ethereum"
                top_n = cls.DEFAULT_TOP_N
                threshold_usd = cls.DEFAULT_THRESHOLD_USD
            elif isinstance(item, dict):
                # 格式3: 完整字典配置
                addr = item.get("address", "")
                chain = item.get("chain", "ethereum")
                top_n = item.get("top_n", cls.DEFAULT_TOP_N)
                threshold_usd = item.get("threshold_usd", cls.DEFAULT_THRESHOLD_USD)
            else:
                print(f"⚠️ 警告: 无法解析的配置项: {item}")
                continue
            
            # 验证链名
            if chain not in cls.SUPPORTED_CHAINS:
                print(f"⚠️ 警告: 不支持的链 '{chain}'，将使用 ethereum")
                chain = "ethereum"
            
            # 验证地址
            if not addr or not addr.startswith("0x"):
                print(f"⚠️ 警告: 无效的地址: {addr}")
                continue
            
            tokens[addr] = {
                "top_n": top_n,
                "threshold_usd": threshold_usd,
                "chain": chain
            }
        
        return tokens
    
    @classmethod
    def get_chain_config(cls, chain_name: str) -> dict:
        """
        获取指定链的配置
        
        Args:
            chain_name: 链名称 (ethereum, bsc, polygon, etc.)
        
        Returns:
            dict: 链配置 {"chain_id": X, "rpc_url": "...", "explorer": "..."}
        """
        return cls.SUPPORTED_CHAINS.get(chain_name, cls.SUPPORTED_CHAINS["ethereum"])
    
    @classmethod
    def get_tokens_by_chain(cls) -> dict:
        """
        按链分组获取 Token 列表
        
        Returns:
            dict: {chain_name: {address: config, ...}, ...}
        """
        tokens = cls.get_target_tokens()
        by_chain = {}
        
        for addr, config in tokens.items():
            chain = config["chain"]
            if chain not in by_chain:
                by_chain[chain] = {}
            by_chain[chain][addr] = config
        
        return by_chain
    
    @classmethod
    def print_config(cls):
        """打印当前配置 (隐藏敏感信息)"""
        print("=" * 50)
        print("当前配置:")
        print("=" * 50)
        
        # 按链分组显示 Token
        tokens_by_chain = cls.get_tokens_by_chain()
        total_tokens = sum(len(t) for t in tokens_by_chain.values())
        print(f"  监控 Token 数量: {total_tokens} (跨 {len(tokens_by_chain)} 条链)")
        
        for chain_name, tokens in tokens_by_chain.items():
            chain_cfg = cls.SUPPORTED_CHAINS.get(chain_name, {})
            chain_display = chain_cfg.get("name", chain_name)
            print(f"\n  📌 {chain_display} (chain_id: {chain_cfg.get('chain_id', '?')}):")
            for i, (addr, cfg) in enumerate(tokens.items(), 1):
                print(f"    [{i}] {addr[:10]}... | Top {cfg['top_n']} | 阈值 ${cfg['threshold_usd']:,.0f}")
        
        print(f"\n  BLOCK_POLL_INTERVAL: {cls.BLOCK_POLL_INTERVAL}s")
        print(f"  WHALE_UPDATE_INTERVAL: {cls.WHALE_UPDATE_INTERVAL}s")
        
        # 显示各链的 RPC 配置状态
        print(f"\n  RPC 配置:")
        for chain_name, chain_cfg in cls.SUPPORTED_CHAINS.items():
            if chain_name in tokens_by_chain:
                print(f"    {chain_cfg['name']}: {cls._mask_url(chain_cfg['rpc_url'])}")
        
        print(f"\n  CHAINBASE_KEY: {'已配置' if cls.CHAINBASE_KEY else '未配置'}")
        print(f"  TG_TOKEN: {'已配置' if cls.TG_TOKEN else '未配置'}")
        print(f"  LOG_LEVEL: {cls.LOG_LEVEL}")
        print("=" * 50)
    
    @staticmethod
    def _mask_url(url: str) -> str:
        """隐藏 URL 中的敏感信息"""
        if not url:
            return "未配置"
        # 只显示域名部分
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}/..."
        except:
            return url[:30] + "..."


def setup_logging() -> logging.Logger:
    """
    配置日志系统，支持文件和控制台输出
    
    Returns:
        logging.Logger: 配置好的日志器
    """
    # 创建格式化器
    formatter = logging.Formatter(
        Config.LOG_FORMAT,
        datefmt=Config.LOG_DATE_FORMAT
    )
    
    # 根日志器
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, Config.LOG_LEVEL.upper()))
    
    # 清除已有的处理器 (避免重复添加)
    root_logger.handlers.clear()
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # 文件处理器 (可选)
    if Config.LOG_FILE:
        file_handler = logging.FileHandler(Config.LOG_FILE, encoding='utf-8')
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    return logging.getLogger(__name__)

