# causal-replica-store-go

纯本地、零外部依赖的多副本因果键值存储。单机内运行多个副本，用进程内
消息通道模拟读写、同步、丢包/重复/乱序与断连重连，使并发冲突、乱序同步
和删除回收后的状态**可预期、可验证**。

## 特性

- **因果版本**：向量时钟；本地读己之写；同键并发更新同时暴露为冲突，
  `Get` 返回 `ErrConflict`，绝不静默覆盖，可用 `Resolve` 显式解决。
- **幂等增量同步**：重复 / 乱序 / 丢失重传 / 中途断开都安全；同步完成后
  双方更新集合一致；断连期间双方写在重连后都保留。
- **删除与安全回收**：墓碑参与因果比较，乱序旧值不复活；仅当全体活跃
  成员都见过删除后才回收；回收水位 `gcFloor` 单调递增。
- **回收后离线回归**：落后于回收水位的副本走确定性的全量快照重同步，
  已删数据不复活；被退役副本只能以新身份重新加入。
- **确定性**：所有网络故障由显式随机种子驱动，同种子多次运行结果一致。

## 快速开始

```bash
go run ./cmd/demo          # 端到端演示
go test -v ./...           # 全部业务用例（含中文业务日志）
CGO_ENABLED=1 go test -race -count=5 ./...
```

## 最小用法

```go
r1 := causal.NewReplica("r1", []string{"r1", "r2"})
r2 := causal.NewReplica("r2", []string{"r1", "r2"})

r1.Put("k", []byte("v1"))
causal.SyncToConvergence(r1, r2, 10) // 增量同步到收敛

v, _ := r2.View("k")                  // 查看键的完整因果视图
```

不可靠网络：

```go
net := causal.NewNetwork([]string{"r1", "r2"}, 128)
net.SetFault("r1", "r2", causal.FaultProfile{Seed: 1, DropRate: 0.3, ReorderRate: 0.3})
net.SetFault("r2", "r1", causal.FaultProfile{Seed: 2, DropRate: 0.3, DupRate: 0.2})
causal.DriveSyncOverNetwork(net, r1, r2, 200) // 故障下多跳收敛
```

## 文档

完整的因果版本模型、冲突/合并规则、同步与回收语义见
[`docs/DESIGN.md`](docs/DESIGN.md)。
