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
    
    # RPC 节点 URL (包含 API Key)
    RPC_URL = os.getenv("RPC_URL", "https://rpc.ankr.com/eth")
    
    # Chainbase API Key (可选，不配置则使用 Ethplorer 免费 API)
    CHAINBASE_KEY = os.getenv("CHAINBASE_API_KEY")
    
    # Telegram Bot 配置
    TG_TOKEN = os.getenv("TG_BOT_TOKEN")
    TG_CHAT_ID = os.getenv("TG_CHAT_ID")
    
    # ============================================================
    # 监控目标配置 (可在此处直接修改)
    # ============================================================
    
    # 要监控的 ERC20 Token 合约地址
    TARGET_TOKEN = "0x6982508145454Ce325dDbE47a25d4ec3d2311933"  # PEPE
    
    # 监控前 N 名持仓大户
    TOP_N = 50
    
    # 警报阈值 (USD)，低于此金额的交易将被忽略
    THRESHOLD_USD = 10000.0
    
    # ============================================================
    # 轮询间隔配置 (秒)
    # ============================================================
    
    # 区块轮询间隔 (以太坊约 12 秒出块)
    BLOCK_POLL_INTERVAL = 12
    
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
    # 设为 None 表示缓存永不过期 (仅作为备份使用)
    CACHE_MAX_AGE = None
    
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
    # 模拟数据 (用于演示/测试，无 Chainbase Key 时使用)
    # ============================================================
    
    MOCK_WHALES = [
        ("0xF977814e90dA44bFA03b6295A0616a897441aceC", 1, 0),  # Binance Hot Wallet
        ("0x5a52E96BAcdaBb82fd05763E25335261B270Efcb", 2, 0),
    ]
    
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
    def print_config(cls):
        """打印当前配置 (隐藏敏感信息)"""
        print("=" * 50)
        print("当前配置:")
        print("=" * 50)
        print(f"  TARGET_TOKEN: {cls.TARGET_TOKEN}")
        print(f"  TOP_N: {cls.TOP_N}")
        print(f"  THRESHOLD_USD: ${cls.THRESHOLD_USD:,.0f}")
        print(f"  BLOCK_POLL_INTERVAL: {cls.BLOCK_POLL_INTERVAL}s")
        print(f"  WHALE_UPDATE_INTERVAL: {cls.WHALE_UPDATE_INTERVAL}s")
        print(f"  RPC_URL: {cls._mask_url(cls.RPC_URL)}")
        print(f"  CHAINBASE_KEY: {'已配置' if cls.CHAINBASE_KEY else '未配置'}")
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

