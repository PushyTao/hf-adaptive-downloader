# hf-adaptive-downloader

下载 Hugging Face 上的整个数据集快照。下载器根据实际吞吐量，在优先下载大文件和优先下载小文件的线程之间调整配比。守护脚本负责在临时故障后重新启动下载器。

## 快速开始

需要 Python 3.8 或更新版本。在仓库目录的 PowerShell 中运行以下命令。

```powershell
python -m pip install -r requirements.txt
python .\watch.py sdu-chenxin/MDS-Bench-data 'F:\MDS_Bench_data' --revision cf27372676d944a103af942fedbc3e2a01a4b546
```

把仓库 ID 和目标目录换成自己的数据集即可。默认将 main 首次解析为提交 SHA，同一目录再次运行会继续该快照。要下载其他仓库或新版本，请指定另一个目录。

官方源默认读取 HF_TOKEN 或 hf auth login 保存的令牌。公开数据集可匿名下载。使用镜像时可以传入 --endpoint https://hf-mirror.com 或设置 HF_ENDPOINT。只有显式设置 HF_ENDPOINT_TOKEN 才会向自定义源发送令牌。

单独运行下载器和执行离线复核的示例命令如下。

```powershell
python .\download.py lhoestq/demo1 'G:\Datasets\demo1'
python .\download.py lhoestq/demo1 'G:\Datasets\demo1' --verify-only
```

首次指定了 --revision 时，离线校验也传入相同的分支、标签或固定 SHA。按 Ctrl+C 可停止，在同一目录重新运行原命令即可续传。

## 调度和进度

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| --workers | 64 | 并发下载的总文件数 |
| --large-workers | 16（总数的四分之一，至少 1） | 初始从队尾取最大文件的线程数，其余线程从队首取最小文件 |
| --window | 300 秒 | 一次稳定配比的观测时间 |
| --fixed-mix | 关闭 | 固定初始配比，可用于对照测试 |

64 线程默认从 16 大、48 小开始，先试探把 4 个名额分给大文件。有效字节吞吐提升超过 5% 时继续沿这个方向试探。在 5% 误差带内，优先选择每秒完成更多小文件（小于 1 MiB）的配比。两者都相近且小文件完成率相同时，优先给小文件分配线程。试探失败会恢复原配比，冷却一个窗口后反向试探。

只有队列有积压、线程用满、实际配比达到目标时才采样。重试、429 和配额变化会清除受影响的样本。支持 Range 的传输可以在写完一个块后让出名额并排回队列，其他传输在文件完成后释放名额。因此日志分别显示实际配比 active L/S 和目标配比 target。

进度约每 15 秒刷新。received 是近期收到并写入临时文件的字节速度，useful 扣除了重新下载时丢弃的字节。ETA 以剩余字节除以近期有效速度，包含已有完整文件、历史断点和正在写入的数据。ETA 只估计传输时间，全盘复核另计。最终以校验报告为准。

所有下载线程、重试和 Range 续传共享解析请求配额。初始按 2970 次/300 秒限速，收到服务器的 RateLimit-Policy 后调整为实际配额的 99%。例如服务端返回 5000 次/300 秒时，目标是 4950。日志中的 resolve 计数只统计当前下载进程，账号的其他进程也可能消耗配额。服务端返回 429 时按响应头要求等待。[Hugging Face 配额说明](https://huggingface.co/docs/hub/en/rate-limits)

## 续传、校验和守护

- 目标目录下的 .hf-adaptive 保存 SQLite 分页清单、临时分片和校验报告。下载前会扫描已有文件并估算缺少的磁盘空间。已存在且大小相符的文件跳过传输，最终仍逐个复核。
- 部分文件按偏移请求 Range，检查返回的 Content-Range。服务端返回完整文件时从头写入。LFS 文件使用 SHA-256，Git 普通文件使用含 blob 头的 SHA-1。临时文件通过哈希校验后才原子替换目标文件。
- 下载结束后回读磁盘，发现不匹配会修复，再写入 verification.json。离线校验发现损坏时返回 76，并将文件名列在 verification-failures.jsonl。
- watch.py 和 download.py 接受相同的参数。守护进程继承终端输出，对暂时性网络故障退避重启，初始等待 30 秒，上限 15 分钟。完整性失败或子进程崩溃会有限重试。认证、磁盘或配置错误会停止并显示原因。用户按 Ctrl+C 时一并停止子进程。
- 同一目录只允许一个下载器和一个守护进程。守护进程每次重启前核对目标目录所在卷及目录身份，检测到磁盘拔出或替换即停止。退出成功前还会核对本次运行的报告、仓库、版本、文件数和字节数。
- 从旧 MDS-Bench 工具切换时，先退出旧守护进程和下载器，使用同一目标目录及原提交 SHA。已落盘的完整文件会复用。两个工具分别存放临时分片。

## 测试

```powershell
python -m unittest discover -s tests -v
```

测试在临时目录启动回环 HTTP 服务，覆盖分页、断点续传、哈希修复、调度决策和守护进程被迫重启子进程后的恢复。
