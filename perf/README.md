# Performance lab

性能实验由四类内容组成：

- `scenarios/cases.json`：请求/响应大小、并发模型、协议与连接布局。
- `suites/*.json`：选择哪些 case，以及 repeat、迭代数、预热和线程数。
- `runner/`：同一进程或远端模式下执行 bRPC、vaelo gRPC 和 tonic 工作负载。
- `baselines/`：准入策略、已接受基线索引和历史证据。

常用入口：

```sh
cargo xtask perf list --suite standard
cargo xtask perf run --suite quick --offline
cargo xtask perf explain target/verification/<run>
cargo xtask perf compare <baseline-id-or-path> target/verification/<run> --gate
cargo xtask perf baseline promote target/verification/<run> --reason "fixed-host scheduled run"
```

`quick` 只验证设施与代表性路径，不能晋升基线。`standard` 是常规固定机器测量，`stress` 增加空消息并扩大迭代、线程与 1 MiB payload。命令行覆盖 suite 参数会记录在 `environment.json`，因此不会与原 suite 静默混淆。

runner 的 window 场景持续维持固定在途请求数；每完成一个请求立即补充一个。报告仅用成功调用计算延迟，失败延迟单独输出；预热或正式调用出现错误时，该轮不能作为基线。
