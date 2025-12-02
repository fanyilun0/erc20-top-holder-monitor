# ERC20 Token 大户监控方案

基于“**优选免费**”和“**契合监控逻辑**”的原则，我们需要重新梳理技术栈。对于大户监控，核心难点在于**“如何免费获取持仓排名”**（RPC节点本身不提供此功能）以及**“如何低成本实时监听”**。

以下是完整的服务选型对比与最终实施方案文档。

---

### 第一部分：功能拆解与服务选型对比

我们需要三个核心组件：**数据源（排名）**、**实时流（监听）**、**辅助数据（价格/标签）**。

| **功能模块** | **需求描述** | **可选服务 (Options)** | **是否免费** | **优缺点分析** | **推荐指数** |
| --- | --- | --- | --- | --- | --- |
| **A. 巨鲸发现**
(Top Holders) | 获取指定 ERC20 代币的持仓前 N 名地址列表。 | **Chainbase** (SQL) | ✅ 免费层级
(2M CU/月) | **优：** 支持 SQL 查询，免费额度大，数据更新快。
**缺：** 需学习简单 SQL。 | ⭐⭐⭐⭐⭐ |
|  |  | **Ethplorer API** | ✅ 免费
(公开接口) | **优：** 接口简单，无需 SQL。
**缺：** 仅限 ETH/BSC 链，QPS 限制较严，不稳定。 | ⭐⭐⭐ |
|  |  | **BitQuery** | ⚠️ 部分免费 | **优：** 数据极全。
**缺：** GraphQL 语法复杂，免费额度极低 (Points制)。 | ⭐⭐ |
| **B. 实时监听**
(Real-time) | 监听链上最新区块的 Transfer 事件。 | **Alchemy** | ✅ 免费层级
(300M CU/月) | **优：** 行业标准，稳定性极高，调试方便。
**缺：** 需自写代码解析 Log。 | ⭐⭐⭐⭐⭐ |
|  |  | **Moralis Streams** | ⚠️ 有限免费 | **优：** 推送式，开发量小。
**缺：** 免费版限制 Stream 数量，不适合大规模监控。 | ⭐⭐⭐ |
|  |  | **Public RPC** | ✅ 免费 | **优：** 无需注册。
**缺：** 极不稳定，速率限制严重，**不可用于生产**。 | ⭐ |
| **C. 价格/元数据**
(Price/Info) | 将代币数量换算为 USD，过滤小额噪音。 | **DeFiLlama** (Coins API) | ✅ 完全免费 | **优：** 无需 Key，无限制，覆盖全。
**缺：** 仅提供价格，无其他元数据。 | ⭐⭐⭐⭐⭐ |
|  |  | **CoinGecko** | ⚠️ 有限免费 | **优：** 数据最全。
**缺：** 免费版 QPS 限制极严 (约 10-30次/分)。 | ⭐⭐⭐ |

---

### 第二部分：最终决策方案

基于上述对比，为了构建一个**零成本、高可用、可扩展**的监控系统，我们采用以下组合：

1. **巨鲸发现 (Discovery):** 使用 **Chainbase (Free Tier)**。
    - *理由：* 它的 SQL API (`SELECT * FROM token_holders`) 可以精准获取排名，且每月 200 万计算单元的免费额度对于“每小时更新一次名单”的需求来说绰绰有余。
2. **实时数据 (Listener):** 使用 **Alchemy (Free Tier)**。
    - *理由：* WebSockets/HTTP 接口极其稳定，免费额度巨大，足够支持毫秒级轮询。
3. **价格换算 (Pricing):** 使用 **DeFiLlama Coins API**。
    - *理由：* 完全免费且不需要 API Key，适合高频调用进行金额过滤。
4. **通知通道:** **Telegram Bot** (完全免费)。

---

### 第三部分：系统实现文档 (Implementation Document)

### 1. 系统架构图

系统采用 **“双线程异步架构”**，将“名单更新”与“实时监听”解耦，确保监控不中断。

代码段

```graphql
graph TD
    subgraph "Thread A: 策略层 (每 1 小时)"
        A1[启动更新任务] --> A2(Chainbase SQL API)
        A2 -->|查询 Top 100 持仓| A3[获取地址列表]
        A3 --> A4{黑名单过滤}
        A4 -->|剔除 交易所/LP/黑洞| A5[更新内存白名单 Set]
    end

    subgraph "Thread B: 监听层 (实时)"
        B1[Alchemy RPC] -->|WebSocket/Loop| B2(监听 Transfer Log)
        B2 --> B3[解析 Log: From/To]
        B3 --> B4{地址在白名单中?}
        B4 -- No --> B1
        B4 -- Yes --> B5(命中逻辑)
    end

    subgraph "Thread C: 处理层 (异步)"
        B5 --> C1[调用 DeFiLlama 获取 Token 价格]
        C1 --> C2{价值 > $10,000 ?}
        C2 -- Yes --> C3[生成富文本报警]
        C3 --> C4[Telegram Bot 推送]
    end
```

### 2. 核心模块逻辑详解

### 模块一：动态名单维护 (Whale Discovery)

- **服务：** Chainbase SQL API
- **执行频率：** 每 30 ~ 60 分钟。
- **SQL 逻辑：**SQL
    
    ```graphql
    SELECT address, original_amount 
    FROM ethereum.token_holders 
    WHERE token_address = '0x你的Token地址' 
    ORDER BY original_amount DESC 
    LIMIT 100
    ```
    
- **清洗规则（至关重要）：**
    - **硬编码过滤：** 排除 `0x00...00` (Zero Address) 和 `0x00...dead` (Burn Address)。
    - **动态过滤（可选）：** 检查该地址是否为合约（通过 RPC `getCode`），通常合约地址是大户（如 Uniswap Pool 或 Staking 合约），若你的目标是监控“个人巨鲸”，则需排除合约地址。

### 模块二：事件监听器 (Event Listener)

- **服务：** Alchemy RPC
- **模式：** 推荐使用 `eth_getLogs` 轮询（每 10-12 秒一次），比 WebSocket 更容易处理断线重连。
- **参数配置：**
    - `address`: 目标 Token CA。
    - `topics`: `[Transfer_Event_Hash]`。
- **匹配算法：**
    - 提取 Log 中的 `from` 和 `to`。
    - 检查：`from` IN `Whale_Set` ? → **减持 (Sell/Move)**。
    - 检查：`to` IN `Whale_Set` ? → **增持 (Buy/In)**。

### 模块三：去噪与增强 (Enrichment)

- **金额计算：**
    - 从 RPC 或本地 ABI 获取 Token `Decimals`。
    - 公式：`Real_Amount = Raw_Value / 10^Decimals`。
- **价值过滤（防骚扰）：**
    - 调用 `https://coins.llama.fi/prices/current/ethereum:0xToken地址`。
    - 计算 `USD_Value = Real_Amount * Price`。
    - **阈值逻辑：** `IF USD_Value > $10,000 THEN Alert ELSE Ignore`。

### 3. 数据结构设计

为了高效匹配，内存中维护以下数据结构：

1. **`Whale_Map` (字典):**
    - Key: `Wallet_Address` (Checksum格式)
    - Value: `{ "rank": 5, "label": "Unknown", "last_balance": 100000 }`
    - *用途：* O(1) 复杂度快速查找地址是否为监控目标，并获取其排名信息。
2. **`Processed_Tx` (集合/LRU缓存):**
    - 存储最近处理过的 `Transaction_Hash`。
    - *用途：* 防止 RPC 节点重组或重复推送导致的消息重复发送。

### 4. 异常处理机制

1. **RPC 限流/超时：**
    - 实施 **指数退避 (Exponential Backoff)** 策略。如果请求失败，等待 1s, 2s, 4s, 8s 后重试。
2. **API 额度耗尽：**
    - 如果 Chainbase 配额耗尽，脚本应降级运行：**停止更新名单，但继续使用旧名单进行监听**，并发送一条系统级警告。
3. **大额误报：**
    - 针对 `Mint` (From=0x0) 和 `Burn` (To=0x0) 事件做特殊标记，通常不视为常规巨鲸异动。

### 5. 输出交付物清单

基于此方案，开发完成后应包含：

1. **`config.py`**: 存放 Token 地址、API Keys、阈值设置。
2. **`whale_loader.py`**: 负责对接 Chainbase 更新名单。
3. **`monitor.py`**: 主程序，负责 RPC 监听和逻辑判断。
4. **`requirements.txt`**: 依赖库 (`web3`, `requests`, `schedule` 等)。

这个方案完全避开了收费服务，同时利用了 Chainbase 强大的 SQL 能力解决了“RPC 无法查排名”的痛点，是一个可长期稳定运行的架构。