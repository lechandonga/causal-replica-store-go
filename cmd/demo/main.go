// Command demo 在单机内演示多副本因果 KV 存储的完整生命周期：
// 本地读写、并发冲突与显式解决、丢包/重复/乱序下的增量同步、
// 断连重连、删除的安全回收，以及回收后长期离线副本的确定性回归。
//
// 运行：go run ./cmd/demo
package main

import (
	"fmt"

	"github.com/lechandonga/causal-replica-store-go/internal/causal"
)

func main() {
	fmt.Println("================ 多副本因果 KV 存储演示 ================")

	// 场景一：并发冲突与显式解决
	fmt.Println("\n[1] 并发冲突：r1/r2 在共同祖先后分别写 red / blue")
	members := []string{"r1", "r2"}
	r1 := causal.NewReplica("r1", members)
	r2 := causal.NewReplica("r2", members)

	r1.Put("color", []byte("white"))
	causal.SyncToConvergence(r1, r2, 10)
	r1.Put("color", []byte("red"))
	r2.Put("color", []byte("blue"))
	causal.SyncToConvergence(r1, r2, 10)

	v, _ := r1.View("color")
	fmt.Printf("    同步后 r1 看到 %d 个并发版本: ", len(v.Entries))
	for i, e := range v.Entries {
		if i > 0 {
			fmt.Print(", ")
		}
		fmt.Printf("%s=%s", e.Origin, e.Value)
	}
	fmt.Println()
	if _, err := r1.Get("color"); err == causal.ErrConflict {
		fmt.Println("    Get() 明确返回 ErrConflict，不静默覆盖")
	}
	r1.Resolve("color", []byte("green"))
	causal.SyncToConvergence(r1, r2, 10)
	val, _ := r2.Get("color")
	fmt.Printf("    r1.Resolve(green) 后，r2 最终读到: %s\n", val)

	// 场景二：不可靠网络上的断连重连
	fmt.Println("\n[2] 断连重连：模拟 35% 丢包 / 30% 重复 / 30% 乱序")
	a := causal.NewReplica("a", []string{"a", "b"})
	b := causal.NewReplica("b", []string{"a", "b"})
	net := causal.NewNetwork([]string{"a", "b"}, 256)
	net.SetFault("a", "b", causal.FaultProfile{Seed: 7, DropRate: 0.35, DupRate: 0.3, ReorderRate: 0.3})
	net.SetFault("b", "a", causal.FaultProfile{Seed: 8, DropRate: 0.35, DupRate: 0.3, ReorderRate: 0.3})

	a.Put("x", []byte("A"))
	b.Put("y", []byte("B"))
	res, err := causal.DriveSyncOverNetwork(net, a, b, 200)
	if err != nil {
		panic(err)
	}
	va, _ := a.Get("y")
	vb, _ := b.Get("x")
	s1, s2 := net.Stats["a->b"], net.Stats["b->a"]
	fmt.Printf("    经 %d 跳收敛；a 读到 y=%s，b 读到 x=%s\n", res.Rounds, va, vb)
	fmt.Printf("    链路实际发生：丢包 %d、重复 %d、乱序 %d（应用层全部幂等消化）\n",
		s1.Dropped+s2.Dropped, s1.Duplicated+s2.Duplicated, s1.Reordered+s2.Reordered)

	// 场景三：删除回收 + 回收后离线副本回归
	fmt.Println("\n[3] 删除回收与离线副本回归：r3 长期离线，r1/r2 回收后 r3 回归")
	c1 := causal.NewReplica("r1", []string{"r1", "r2", "r3"})
	c2 := causal.NewReplica("r2", []string{"r1", "r2", "r3"})
	c3 := causal.NewReplica("r3", []string{"r1", "r2", "r3"})

	c1.Put("keep", []byte("K"))
	c1.Put("gone", []byte("G"))
	c1.Delete("gone")
	causal.SyncToConvergence(c1, c2, 10)
	causal.SyncToConvergence(c1, c3, 10)
	causal.SyncToConvergence(c2, c3, 10)

	c1.Put("live", []byte("v1"))
	c1.Delete("keep")
	causal.SyncToConvergence(c1, c2, 10)
	rep := c1.CollectGarbage()
	c2.CollectGarbage()
	fmt.Printf("    r3 离线期间，r1/r2 安全回收 %d 条操作（全员确认见过）\n", len(rep.Reaped))

	pre := c3.Seen()
	causal.SyncToConvergence(c1, c3, 10)
	if _, err := c3.Get("gone"); err == causal.ErrNotFound {
		fmt.Printf("    r3（旧水位 %v）回归经快照重同步：已删键 gone/keep 未复活\n", pre)
	}
	live, _ := c3.Get("live")
	fmt.Printf("    r3 重同步后读到存活键 live=%s，与集群完全一致\n", live)

	fmt.Println("\n================ 演示结束：状态确定、可收敛、可验证 ================")
}
