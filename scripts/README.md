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
