# GitHub Actions K 线快照迁移

`.github/workflows/kline-snapshot.yml` 使用无 Token 的东方财富历史接口生成 Lemon Light 的 `.ksnap` 快照，并通过工作流自带的 `GITHUB_TOKEN` 上传到当前仓库的 `kline-snapshot` Release。

首次运行：

1. 在 `scripts/symbols.txt` 填入需要 30 分钟历史的标的（例如 `sz000938`）。
2. GitHub 仓库 Settings → Actions → General，确认 Workflow permissions 为 **Read and write permissions**。
3. Actions → K-line snapshot → Run workflow，首次保持 `all_a_shares=true`。
4. 等待任务完成，Release 中会出现 `kline-snapshot-YYYYMMDD.ksnap`。

日线/周线默认拉全体 A 股；30 分钟线只拉 `symbols.txt`，避免全市场分钟数据造成数万次请求。当前脚本每次按接口上限重新拉取完整窗口，首次全量回补完成后建议保留自选标的在 `symbols.txt`，再根据运行耗时决定是否把全市场任务改为手动执行。

全市场 30 分钟线可以用工作流的 `all_m30=true` 分批补齐：先运行一次日线/周线初始化，再手动依次运行 `batch_index=0,1,2...`，`batch_size` 建议 50-100。每次运行会先恢复上一版 Release 快照，只替换当前批次并重新上传，所以不会丢掉前面批次。东方财富此处每只股票只取接口允许的最近 320 根 30 分钟 K 线；这不是完整多年分钟历史，完整历史需要支持分页/授权的分钟数据源。

工作流默认使用 2 个并发、每次请求前约 0.8 秒间隔，以降低东方财富网关 502/429 的概率；如果仍被限流，可在 workflow 中继续调低 `--workers` 或调高 `--delay`。

朋友端仍然使用安装包内置的 Contents 只读 Token；不要把写权限 PAT 写进代码、`scripts/` 或仓库文件。

## 复权修复（2026-11-20）

通达信 `.day` 包是原始价格，不能以单个重叠日期的比例换算全部历史。该灌底路径已停用。
日线完整拉取同一提供方的前复权序列，保留合法负价；周月线由日线聚合。
缓存使用 `adjustment.schema=2` 记录来源和口径；不同来源、基准变化及未验证旧历史禁止直接拼接。

手动运行 **Repair price adjustment** 工作流：恢复原始快照，完整重建所有股票和 ETF，按同日日线核对分钟历史。
分钟转换仅用于能用原始日线的价格范围、开收盘锚点验证的交易日；其余历史保留原件并隔离。
日常 GitHub Actions 保留定时运行，先发布最新日线供筛选，再执行分钟和历史补修。异常文件单独隔离，绝不阻塞正常数据发布。
近期已验证日线可先发布供筛选，旧历史另存 `quarantinedHistory`，逐步完整重建；新鲜度、历史覆盖和隔离数量写入快照 quality 与工作流 artifact。
修复报告及原始快照保留在 workflow artifact，未验证数据不会被静默裁掉。
本地也可运行 `python3 scripts/repair_adjustment.py --cache PATH --backup BACKUP --report REPORT.json --workers 4`。

失败恢复：Actions 缓存已完成的校验数据；恢复 Release 时不覆盖缓存中更新的已验证文件。
发布先上传暂存资产，验证大小后再切换正式名称；切换失败尝试恢复旧名称，成功之前不删除旧资产。
`Retry failed snapshot once` 仅对 main 分支首次失败自动重试一次，避免永久循环。质量报告和日志保留为工作流 artifact。
