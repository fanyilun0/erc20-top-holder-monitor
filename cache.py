"""
本地缓存管理器

支持多 Token 的大户名单本地持久化
- 成功获取数据后自动保存到本地
- API 请求失败时自动加载本地缓存
- 支持缓存过期检查
"""

import json
import os
import time
import threading
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pathlib import Path


class WhaleCache:
    """
    大户名单本地缓存管理器
    
    缓存文件结构:
    {
        "token_address": "0x...",
        "symbol": "PEPE",
        "updated_at": 1701234567.89,
        "source": "chainbase",  # chainbase / ethplorer / mock
        "holders": [
            {"address": "0x...", "rank": 1, "balance": 123456.78},
            ...
        ]
    }
    """
    
    def __init__(self, cache_dir: str = "cache"):
        """
        初始化缓存管理器
        
        Args:
            cache_dir: 缓存目录路径
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
    
    def _get_cache_path(self, token_address: str) -> Path:
        """获取指定 Token 的缓存文件路径"""
        # 使用 Token 地址作为文件名 (小写，去掉 0x 前缀)
        safe_name = token_address.lower().replace("0x", "")
        return self.cache_dir / f"holders_{safe_name}.json"
    
    def save(
        self, 
        token_address: str, 
        holders: List[Tuple[str, int, float]], 
        symbol: str = "UNKNOWN",
        source: str = "unknown",
        decimals: int = 18
    ) -> bool:
        """
        保存大户名单到本地缓存
        
        Args:
            token_address: Token 合约地址
            holders: 大户列表 [(address, rank, balance), ...]
            symbol: Token 符号
            source: 数据来源 (chainbase/ethplorer/mock)
            decimals: Token 精度，用于计算可读余额
        
        Returns:
            bool: 保存是否成功
        """
        cache_path = self._get_cache_path(token_address)
        
        cache_data = {
            "token_address": token_address.lower(),
            "symbol": symbol,
            "decimals": decimals,
            "updated_at": time.time(),
            "updated_at_str": datetime.now().isoformat(),
            "source": source,
            "holders_count": len(holders),
            "holders": [
                {
                    "address": addr,
                    "rank": rank,
                    "balance": balance,
                    "readableBalance": balance / (10 ** decimals)
                }
                for addr, rank, balance in holders
            ]
        }
        
        try:
            with self._lock:
                # 先写入临时文件，再原子重命名
                temp_path = cache_path.with_suffix('.tmp')
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(cache_data, f, indent=2, ensure_ascii=False)
                temp_path.replace(cache_path)
            return True
        except Exception as e:
            print(f"[Cache] 保存缓存失败: {e}")
            return False
    
    def load(
        self, 
        token_address: str, 
        max_age_seconds: Optional[float] = None
    ) -> Optional[Dict]:
        """
        从本地缓存加载大户名单
        
        Args:
            token_address: Token 合约地址
            max_age_seconds: 最大缓存年龄 (秒)，超过则视为过期，None 表示不检查
        
        Returns:
            Dict: 缓存数据，如果不存在或已过期则返回 None
        """
        cache_path = self._get_cache_path(token_address)
        
        if not cache_path.exists():
            return None
        
        try:
            with self._lock:
                with open(cache_path, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
            
            # 检查缓存是否过期
            if max_age_seconds is not None:
                age = time.time() - cache_data.get('updated_at', 0)
                if age > max_age_seconds:
                    return None
            
            return cache_data
            
        except Exception as e:
            print(f"[Cache] 加载缓存失败: {e}")
            return None
    
    def load_holders(
        self, 
        token_address: str,
        max_age_seconds: Optional[float] = None
    ) -> Optional[List[Tuple[str, int, float]]]:
        """
        从本地缓存加载大户列表 (简化格式)
        
        Args:
            token_address: Token 合约地址
            max_age_seconds: 最大缓存年龄 (秒)
        
        Returns:
            List[Tuple]: [(address, rank, balance), ...] 或 None
        """
        cache_data = self.load(token_address, max_age_seconds)
        if not cache_data:
            return None
        
        holders = cache_data.get('holders', [])
        return [
            (h['address'], h['rank'], h.get('balance', 0))
            for h in holders
        ]
    
    def get_cache_info(self, token_address: str) -> Optional[Dict]:
        """
        获取缓存元信息 (不加载完整数据)
        
        Returns:
            Dict: {"updated_at": ..., "source": ..., "holders_count": ...}
        """
        cache_data = self.load(token_address)
        if not cache_data:
            return None
        
        return {
            "token_address": cache_data.get('token_address'),
            "symbol": cache_data.get('symbol'),
            "updated_at": cache_data.get('updated_at'),
            "updated_at_str": cache_data.get('updated_at_str'),
            "source": cache_data.get('source'),
            "holders_count": cache_data.get('holders_count', len(cache_data.get('holders', [])))
        }
    
    def exists(self, token_address: str) -> bool:
        """检查缓存是否存在"""
        return self._get_cache_path(token_address).exists()
    
    def delete(self, token_address: str) -> bool:
        """删除指定 Token 的缓存"""
        cache_path = self._get_cache_path(token_address)
        try:
            if cache_path.exists():
                cache_path.unlink()
            return True
        except Exception as e:
            print(f"[Cache] 删除缓存失败: {e}")
            return False
    
    def list_cached_tokens(self) -> List[str]:
        """列出所有已缓存的 Token 地址"""
        tokens = []
        for cache_file in self.cache_dir.glob("holders_*.json"):
            # 从文件名解析 Token 地址
            name = cache_file.stem.replace("holders_", "")
            tokens.append(f"0x{name}")
        return tokens
    
    def clear_all(self) -> int:
        """清除所有缓存"""
        count = 0
        for cache_file in self.cache_dir.glob("holders_*.json"):
            try:
                cache_file.unlink()
                count += 1
            except:
                pass
        return count


# 全局缓存实例
_cache_instance: Optional[WhaleCache] = None


def get_cache(cache_dir: str = "cache") -> WhaleCache:
    """获取全局缓存实例 (单例模式)"""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = WhaleCache(cache_dir)
    return _cache_instance

