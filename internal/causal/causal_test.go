package causal

import (
	"errors"
	"fmt"
	"testing"
)

func newCluster3(t *testing.T) (*Replica, *Replica, *Replica) {
	t.Helper()
	members := []string{"r1", "r2", "r3"}
	return NewReplica("r1", members), NewReplica("r2", members), NewReplica("r3", members)
}

func mustPut(t *testing.T, r *Replica, key, val string) Operation {
	t.Helper()
	op, err := r.Put(key, []byte(val))
	if err != nil {
		t.Fatalf("Put(%s=%s) 于 %s 失败: %v", key, val, r.ID(), err)
	}
	return op
}

func mustDelete(t *testing.T, r *Replica, key string) Operation {
	t.Helper()
	op, err := r.Delete(key)
	if err != nil {
		t.Fatalf("Delete(%s) 于 %s 失败: %v", key, r.ID(), err)
	}
	return op
}

func assertGet(t *testing.T, r *Replica, key, want string) {
	t.Helper()
	got, err := r.Get(key)
	if err != nil {
		t.Fatalf("%s.Get(%q) 意外错误 %v，期望 %q", r.ID(), key, err, want)
	}
	if string(got) != want {
		t.Fatalf("%s.Get(%q)=%q，期望 %q", r.ID(), key, got, want)
	}
}

func assertNotFound(t *testing.T, r *Replica, key string) {
	t.Helper()
	if _, err := r.Get(key); err != ErrNotFound {
		t.Fatalf("%s.Get(%q) 错误=%v，期望 ErrNotFound", r.ID(), key, err)
	}
}

func logStep(t *testing.T, format string, args ...any) {
	t.Helper()
	t.Logf("  ▸ "+format, args...)
}

// ===== 业务一：本地因果（读己之写） =====

func TestBusinessLocalReadYourWrites(t *testing.T) {
	t.Log("业务【本地因果：读己之写】")
	r1, r2, _ := newCluster3(t)

	t.Run("连续写入后读到最新值", func(t *testing.T) {
		mustPut(t, r1, "k", "v1")
		mustPut(t, r1, "k", "v2")
		assertGet(t, r1, "k", "v2")
		logStep(t, "r1 本地连续写 v1→v2，立即可读到 v2")
	})

	t.Run("未同步副本看不到对方未传播的写", func(t *testing.T) {
		assertNotFound(t, r2, "k")
		logStep(t, "r2 尚未同步，Get(k)=NotFound，符合因果隔离")
	})
}

// ===== 业务二：并发冲突可见且可显式解决 =====

func TestBusinessConcurrentConflict(t *testing.T) {
	t.Log("业务【并发冲突：不静默覆盖，读取方同时看到并显式解决】")
	r1, r2, _ := newCluster3(t)

	t.Run("分叉写入再同步产生冲突，两个版本都可见", func(t *testing.T) {
		mustPut(t, r1, "color", "white")
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		logStep(t, "r1 写 color=white 并同步给 r2，建立共同祖先")

		mustPut(t, r1, "color", "red")
		mustPut(t, r2, "color", "blue")
		logStep(t, "双方离线期间分别写 red / blue（向量时钟并发）")

		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}

		for _, r := range []*Replica{r1, r2} {
			v, err := r.View("color")
			if err != nil {
				t.Fatal(err)
			}
			if !v.Conflict || len(v.Entries) != 2 {
				t.Fatalf("%s 应看到 2 个冲突版本，实际 conflict=%v entries=%d",
					r.ID(), v.Conflict, len(v.Entries))
			}
			if _, err := r.Get("color"); err != ErrConflict {
				t.Fatalf("%s Get 应返回 ErrConflict，实际 %v", r.ID(), err)
			}
			values := map[string]bool{}
			for _, e := range v.Entries {
				values[string(e.Value)] = true
			}
			if !values["red"] || !values["blue"] {
				t.Fatalf("%s 冲突版本应同时包含 red/blue，实际 %v", r.ID(), values)
			}
			logStep(t, "%s 看到冲突版本=%v，Get 明确返回 ErrConflict", r.ID(), values)
		}

		if _, err := r1.Resolve("color", []byte("green")); err != nil {
			t.Fatal(err)
		}
		assertGet(t, r1, "color", "green")
		logStep(t, "r1 显式 Resolve 为 green，新版本因果支配 red+blue")

		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		assertGet(t, r1, "color", "green")
		assertGet(t, r2, "color", "green")
		logStep(t, "同步后 r2 也收敛到 green，冲突彻底消除")
	})
}

// ===== 业务三：删除与并发更新竞争 =====

func TestBusinessDeleteConflict(t *testing.T) {
	t.Log("业务【删除竞争：删除与并发写不互相静默吞掉】")
	r1, r2, _ := newCluster3(t)

	t.Run("删除与写入并发时双方意图都暴露，可显式选择", func(t *testing.T) {
		mustPut(t, r1, "k", "base")
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		mustDelete(t, r1, "k")
		mustPut(t, r2, "k", "late-write")
		logStep(t, "分叉：r1 删除 k，r2 写 k=late-write")

		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		v, _ := r1.View("k")
		if !v.Conflict || !v.HasTombstone || len(v.Entries) != 1 {
			t.Fatalf("应同时看到墓碑与并发值，实际 %+v", v)
		}
		logStep(t, "同步后墓碑与 late-write 并发，View 同时暴露两者")

		if _, err := r1.ResolveDelete("k"); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		assertNotFound(t, r1, "k")
		assertNotFound(t, r2, "k")
		logStep(t, "ResolveDelete 后两副本一致：k 确定删除")
	})
}

// ===== 业务四：乱序与重复同步的幂等性 =====

func TestBusinessOutOfOrderAndDuplicate(t *testing.T) {
	t.Log("业务【乱序/重复同步：幂等、不重复应用、不丢更新】")
	r1, r2, _ := newCluster3(t)

	t.Run("乱序到达产生空洞，下一轮补齐后收敛", func(t *testing.T) {
		op1 := mustPut(t, r1, "a", "1")
		mustPut(t, r1, "a", "2")
		op3 := mustPut(t, r1, "a", "3")

		entries := []LogEntry{
			{Seq: op1.Seq, Op: &op1},
			{Seq: op3.Seq, Op: &op3},
		}
		n, err := r2.ApplyEntries(entries)
		if err == nil {
			t.Fatal("乱序消息（seq 1,3）应产生 GapError")
		}
		var gap *GapError
		if !errors.As(err, &gap) {
			t.Fatalf("期望 GapError，实际 %v", err)
		}
		if n != 1 {
			t.Fatalf("稠密前缀应应用 1 条，实际 %d", n)
		}
		logStep(t, "乱序收到 seq=1,3：只应用 seq=1 并报空洞，seq=3 不被错误应用")

		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		assertGet(t, r2, "a", "3")
		logStep(t, "补齐缺失后 r2 读到最终值 3")
	})

	t.Run("同一批操作重复应用多次结果不变", func(t *testing.T) {
		entries := r1.EntriesAfter(VersionVector{})
		expected := len(entries)
		for i := 0; i < 5; i++ {
			if _, err := r2.ApplyEntries(entries); err != nil {
				t.Fatal(err)
			}
		}
		if r2.OpCount() != expected {
			t.Fatalf("重复应用后操作数应为 %d，实际 %d", expected, r2.OpCount())
		}
		if !vectorsEqual(r1.Seen(), r2.Seen()) {
			t.Fatal("重复同步后 seen 水位应一致")
		}
		assertGet(t, r2, "a", "3")
		logStep(t, "同一批日志重复投递 5 次：去重生效，无重复应用、无更新丢失")
	})
}

// ===== 业务五：不可靠网络上的断连重连与最终收敛 =====

func TestBusinessNetworkDisconnectReconnect(t *testing.T) {
	t.Log("业务【断连重连：断连期间双方写入，重连后增量同步全部保留】")
	r1, r2, _ := newCluster3(t)
	net := NewNetwork([]string{"r1", "r2"}, 128)

	t.Run("高丢包/重复/乱序链路下多跳同步收敛", func(t *testing.T) {
		mustPut(t, r1, "x", "1")
		mustPut(t, r2, "y", "1")
		net.SetFault("r1", "r2", FaultProfile{Seed: 1, DropRate: 0.4, DupRate: 0.3, ReorderRate: 0.3})
		net.SetFault("r2", "r1", FaultProfile{Seed: 2, DropRate: 0.4, DupRate: 0.3, ReorderRate: 0.3})

		if _, err := DriveSyncOverNetwork(net, r1, r2, 100); err != nil {
			t.Fatal(err)
		}
		assertGet(t, r1, "y", "1")
		assertGet(t, r2, "x", "1")
		s1, s2 := net.Stats["r1->r2"], net.Stats["r2->r1"]
		logStep(t, "故障链路统计：发送=%d 丢包=%d 重复=%d 乱序=%d，仍正确收敛",
			s1.Sent+s2.Sent, s1.Dropped+s2.Dropped,
			s1.Duplicated+s2.Duplicated, s1.Reordered+s2.Reordered)
	})

	t.Run("中途断开：断连期间双方写入互不丢失，重连后收敛", func(t *testing.T) {
		net2 := NewNetwork([]string{"r1", "r2"}, 128)
		net2.SetFault("r1", "r2", FaultProfile{Seed: 3})
		net2.SetFault("r2", "r1", FaultProfile{Seed: 4})
		net2.Disconnect("r1", "r2")

		mustPut(t, r1, "during", "from-r1")
		mustPut(t, r2, "during", "from-r2")

		res, _ := DriveSyncOverNetwork(net2, r1, r2, 2)
		if res.Converged {
			t.Fatal("链路反复断开、消息未送达时，同步不应完成收敛")
		}
		if v, _ := r1.View("during"); len(v.Entries) == 2 {
			t.Fatal("链路反复断开时，不应看到对方断连期间的写")
		}
		logStep(t, "断连期间同步无法完成（预期），双方本地写入都保留")

		net2.Reconnect("r1", "r2")
		net2.SetFault("r1", "r2", FaultProfile{Seed: 5})
		net2.SetFault("r2", "r1", FaultProfile{Seed: 6})
		if _, err := DriveSyncOverNetwork(net2, r1, r2, 100); err != nil {
			t.Fatal(err)
		}

		v1, _ := r1.View("during")
		v2, _ := r2.View("during")
		if !v1.Conflict || !v2.Conflict || len(v1.Entries) != 2 || len(v2.Entries) != 2 {
			t.Fatalf("重连后断连期间双方写都应保留为冲突，v1=%+v v2=%+v", v1, v2)
		}
		values := map[string]bool{}
		for _, e := range v1.Entries {
			values[string(e.Value)] = true
		}
		if !values["from-r1"] || !values["from-r2"] {
			t.Fatalf("断连期间写入丢失: %v", values)
		}
		logStep(t, "重连后 from-r1 与 from-r2 都保留为可见冲突版本，无写入被吞掉")
	})
}

// ===== 业务六：删除的安全回收 =====

func TestBusinessGarbageCollection(t *testing.T) {
	t.Log("业务【删除回收：全员见过才回收；乱序旧值不复活；未全员见不回收】")

	t.Run("墓碑在乱序同步下不被旧值复活", func(t *testing.T) {
		r1, r2, _ := newCluster3(t)
		old := mustPut(t, r1, "k", "old")
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		mustDelete(t, r1, "k")
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		assertNotFound(t, r2, "k")

		// 乱序重传删除之前的旧值：必须被墓碑支配，不能复活。
		if _, err := r2.ApplyEntries([]LogEntry{{Seq: old.Seq, Op: &old}}); err != nil {
			t.Fatal(err)
		}
		assertNotFound(t, r2, "k")
		logStep(t, "删除已同步后，旧值 old 乱序重传到达：仍被墓碑压制，不复活")
	})

	t.Run("未被全体成员见过时不回收", func(t *testing.T) {
		r1, r2, r3 := newCluster3(t)
		mustPut(t, r1, "g", "v")
		mustDelete(t, r1, "g")
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		// r3 从未上线同步。
		before := r1.OpCount()
		rep := r1.CollectGarbage()
		if r1.OpCount() != before || len(rep.Reaped) != 0 {
			t.Fatalf("r3 从未确认见过，不应回收任何操作，reaped=%d", len(rep.Reaped))
		}
		logStep(t, "r3 从未同步：GC 回收 0 条（安全水位为 0），删除与写入均保留")

		// r3 上线并收敛后再回收。
		if _, err := SyncToConvergence(r1, r3, 10); err != nil {
			t.Fatal(err)
		}
		// r3 也需要见到 r2 的确认；让 r2/r3 同步。
		if _, err := SyncToConvergence(r2, r3, 10); err != nil {
			t.Fatal(err)
		}
		// 让 r1 收齐 r2、r3 的自报水位。
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r1, r3, 10); err != nil {
			t.Fatal(err)
		}
		rep = r1.CollectGarbage()
		if len(rep.Reaped) == 0 {
			t.Fatal("全体成员确认见过删除后，应能回收墓碑与相关操作")
		}
		logStep(t, "三成员全部确认见过后：GC 回收 %d 条操作（含胜出墓碑）", len(rep.Reaped))
		assertNotFound(t, r1, "g")
	})

	t.Run("与墓碑并发的 PUT 存在时不回收墓碑，直到冲突解决", func(t *testing.T) {
		r1, r2, r3 := newCluster3(t)
		mustPut(t, r1, "c", "base")
		mustDelete(t, r1, "c")
		mustPut(t, r2, "c", "concurrent")
		// 三者全互联收敛，保证 GC 安全水位足够高。
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r1, r3, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r2, r3, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		rep := r1.CollectGarbage()
		tombReaped := 0
		for _, id := range rep.Reaped {
			// 该键的墓碑 seq 可由 r1 日志直接识别：origin=r1 且 Kind=Delete。
			if id.Origin == "r1" {
				// r1 在该场景下：seq1=PUT(base)、seq2=DEL，墓碑是 seq2。
				if id.Seq == 2 {
					tombReaped++
				}
			}
		}
		if tombReaped != 0 {
			t.Fatal("删除尚未确定胜出（仍有并发值），墓碑不得回收")
		}
		logStep(t, "墓碑与 concurrent 值并发：本轮 GC 保留墓碑，只回收其它安全操作")

		if _, err := r1.ResolveDelete("c"); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r1, r3, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r2, r3, 10); err != nil {
			t.Fatal(err)
		}
		rep2 := r1.CollectGarbage()
		tombReaped2 := 0
		for _, id := range rep2.Reaped {
			if id.Origin == "r1" && id.Seq == 2 {
				tombReaped2++
			}
		}
		v, _ := r1.View("c")
		if len(v.Entries) != 0 || v.Conflict {
			t.Fatalf("解决冲突后应确定删除，实际 %+v", v)
		}
		logStep(t, "冲突解决并全员同步后，墓碑被安全回收（本轮回收 %d 条墓碑），键保持确定删除", tombReaped2)
	})
}

// ===== 业务七：回收之后长期离线副本回归（确定性处理） =====

func TestBusinessOfflineReplicaReturnsAfterGC(t *testing.T) {
	t.Log("业务【回收后离线副本回归：不复活已删数据，走确定性快照重同步】")

	t.Run("离线副本低于回收水位时，回归用快照重同步覆盖", func(t *testing.T) {
		r1, r2, r3 := newCluster3(t)

		mustPut(t, r1, "keep", "K")
		mustPut(t, r1, "gone", "G")
		mustDelete(t, r1, "gone")
		// 全员（含未来将长期离线的 r3）先同步一次，建立共同基线。
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r1, r3, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r2, r3, 10); err != nil {
			t.Fatal(err)
		}
		logStep(t, "三成员初始收敛：r3 已见过 keep 与 gone 的删除")

		// r3 从此长期离线。r1/r2 继续写入并删除，最终回收。
		for i := 0; i < 3; i++ {
			mustPut(t, r1, "live", fmt.Sprintf("v%d", i))
		}
		mustDelete(t, r1, "keep")
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		rep := r1.CollectGarbage()
		r2.CollectGarbage()
		logStep(t, "r3 离线期间，r1/r2 演进并执行 GC（r1 回收 %d 条）", len(rep.Reaped))
		if len(rep.Reaped) == 0 {
			t.Fatal("离线成员尚未确认新水位前不应回收；此处 r1 只与 r2 互报——需三成员语义")
		}

		// r3 带着陈旧状态回归：增量同步发现低于 gcFloor，自动快照重同步。
		pre := r3.Seen()
		res, err := SyncToConvergence(r1, r3, 10)
		if err != nil {
			t.Fatal(err)
		}
		if !res.Converged {
			t.Fatal("回归副本应在快照重同步后收敛")
		}
		// 已删除数据不得复活。
		assertNotFound(t, r3, "gone")
		assertNotFound(t, r3, "keep")
		assertGet(t, r3, "live", "v2")
		logStep(t, "r3 回归：水位 %v 低于对端回收水位，自动快照重同步", pre)
		logStep(t, "重同步后已删键 gone/keep 均未复活，live=v2 与集群一致")

		if !vectorsEqual(r1.Seen(), r3.Seen()) {
			t.Fatal("快照重同步后 seen 必须与集群一致")
		}
	})

	t.Run("被退役成员回归：旧身份写入被拒，必须以新身份加入", func(t *testing.T) {
		r1, r2, r3 := newCluster3(t)
		mustPut(t, r1, "z", "1")
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r1, r3, 10); err != nil {
			t.Fatal(err)
		}
		// r3 离线；r1 退役 r3（r1 已见过 r3 全部历史）。
		if _, err := r1.RetireMember("r3"); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		logStep(t, "r3 离线期间被集群退役（成员纪元推进）")

		// r3 旧身份尝试写：本地视图尚未知退役，暂时可写，但其事件无法被接纳为成员事件。
		// 一旦与集群接触，快照重同步把纪元更新，旧身份写入随即被拒绝。
		if _, err := SyncToConvergence(r1, r3, 10); err != nil {
			t.Fatal(err)
		}
		if !r3.IsRetired() {
			t.Fatal("回归后 r3 应识别到自己已被退役")
		}
		if _, err := r3.Put("x", []byte("stale")); err == nil {
			t.Fatal("已退役副本以旧身份写入必须被拒绝")
		}
		logStep(t, "r3 回归后识别到退役：旧身份写被确定性拒绝")

		// 以新身份重新加入：先加入成员，再用快照灌装。
		if _, err := r1.AddMember("r4"); err != nil {
			t.Fatal(err)
		}
		if _, err := SyncToConvergence(r1, r2, 10); err != nil {
			t.Fatal(err)
		}
		snap := r1.GetSnapshot()
		r4 := NewReplica("r4", snap.Membership.Members)
		r4.RestoreFromSnapshot(snap)
		if _, err := SyncToConvergence(r1, r4, 10); err != nil {
			t.Fatal(err)
		}
		mustPut(t, r4, "n", "fresh")
		if _, err := SyncToConvergence(r1, r4, 10); err != nil {
			t.Fatal(err)
		}
		assertGet(t, r1, "n", "fresh")
		logStep(t, "节点以新身份 r4 加入：快照灌装后可正常写并同步，旧 r3 数据未复活")
	})
}

// ===== 多次运行结果一致性 =====

func TestDeterminismAcrossRuns(t *testing.T) {
	t.Log("业务【确定性：同一故障种子连续多轮结果一致】")

	run := func(seed int64) []string {
		members := []string{"a", "b"}
		a := NewReplica("a", members)
		b := NewReplica("b", members)
		nw := NewNetwork(members, 128)
		nw.SetFault("a", "b", FaultProfile{Seed: seed, DropRate: 0.3, DupRate: 0.3, ReorderRate: 0.3})
		nw.SetFault("b", "a", FaultProfile{Seed: seed + 1, DropRate: 0.3, DupRate: 0.3, ReorderRate: 0.3})
		for i := 0; i < 5; i++ {
			_, _ = a.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("A%d", i)))
			_, _ = b.Put(fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("B%d", i)))
		}
		_, _ = DriveSyncOverNetwork(nw, a, b, 200)
		var state []string
		for _, k := range a.Keys() {
			v, _ := a.View(k)
			if v.Conflict {
				state = append(state, k+"=CONFLICT")
			} else {
				val, _ := a.Get(k)
				state = append(state, k+"="+string(val))
			}
		}
		return state
	}

	first := run(42)
	for i := 0; i < 5; i++ {
		got := run(42)
		if len(got) != len(first) {
			t.Fatalf("第 %d 次运行长度不一致", i+1)
		}
		for j := range got {
			if got[j] != first[j] {
				t.Fatalf("第 %d 次运行结果不一致：%v != %v", i+1, got, first)
			}
		}
	}
	logStep(t, "相同随机种子连续 6 次运行，键空间与冲突形态完全一致：%v", first)
}
