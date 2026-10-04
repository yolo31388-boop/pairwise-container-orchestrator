"""
游戏内容器编排与调度系统 - 验收测试
运行方式：python -m pytest tests/test_container_orchestrator.py -q
共 15 个测试用例
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from container.orchestrator import ContainerOrchestrator


class TestContainerOrchestrator:
    def setup_method(self):
        self.system = ContainerOrchestrator()

    # ---- 调度：资源与负载感知 ----

    def test_case_01(self):
        """调度必须考虑节点剩余资源，放不下的容器不能硬塞（禁止超配）。"""
        s = self.system
        s.add_node("n1", cpu=4, mem=8)
        s.add_node("n2", cpu=4, mem=8)
        n_a = s.schedule_container("a", cpu_req=3, mem_req=2)
        n_b = s.schedule_container("b", cpu_req=3, mem_req=2)
        assert n_a is not None and n_b is not None
        assert n_a != n_b  # 单节点放不下两个 3核，必须分开
        assert s.schedule_container("c", cpu_req=8, mem_req=1) is None  # 超出所有节点容量

    def test_case_02(self):
        """调度必须考虑负载：优先选择负载最低的节点，而不是简单轮询。"""
        s = self.system
        s.add_node("n1", cpu=10, mem=10)
        s.add_node("n2", cpu=4, mem=10)
        s.add_node("n3", cpu=4, mem=10)
        n_a = s.schedule_container("a", cpu_req=2)
        n_b = s.schedule_container("b", cpu_req=2)
        # n1 容量大、利用率始终最低，负载感知调度应连续选中 n1；轮询则会散开
        assert n_a == "n1"
        assert n_b == "n1"

    # ---- 调度：亲和性 / 反亲和性 ----

    def test_case_03(self):
        """亲和性：相关联的容器应调度到同一台机器。"""
        s = self.system
        s.add_node("n1", cpu=4, mem=8)
        s.add_node("n2", cpu=4, mem=8)
        s.schedule_container("filler", cpu_req=3)          # 占住 n1
        db_node = s.schedule_container("db", service="db", cpu_req=1)
        assert db_node == "n2"
        app_node = s.schedule_container("app", service="app", cpu_req=1,
                                        affinity=["db"])
        assert app_node == db_node  # app 与 db 同机

    def test_case_04(self):
        """反亲和性：同一服务的多个副本必须分散到不同机器。"""
        s = self.system
        for nid in ("n1", "n2", "n3"):
            s.add_node(nid, cpu=4, mem=8)
        nodes = [
            s.schedule_container("r%d" % i, service="game", cpu_req=1,
                                 anti_affinity=True)
            for i in range(3)
        ]
        assert all(n is not None for n in nodes)
        assert len(set(nodes)) == 3  # 三个副本三台机器

    # ---- 资源管理：限额、监控、防超配 ----

    def test_case_05(self):
        """资源限额：容器超过限额必须被检测出来。"""
        s = self.system
        s.add_node("n1", cpu=8, mem=8)
        s.schedule_container("c1", cpu_req=2)
        s.schedule_container("c2", cpu_req=2)
        s.set_resource_limit("c1", cpu=1.0)
        s.set_resource_limit("c2", cpu=4.0)
        s.report_usage("c1", cpu=2.0)   # 超过 1.0 限额
        s.report_usage("c2", cpu=3.0)   # 未超
        report = s.resource_manager()
        assert report["limits"]["c1"]["cpu_limit"] == 1.0
        assert "c1" in report["violations"]
        assert "c2" not in report["violations"]

    def test_case_06(self):
        """资源监控：必须能看到节点分配量与实际使用率。"""
        s = self.system
        s.add_node("n1", cpu=4, mem=8)
        s.schedule_container("c1", cpu_req=1, mem_req=2)
        s.schedule_container("c2", cpu_req=1, mem_req=2)
        s.report_usage("c1", cpu=0.5, mem=1.0)
        s.report_usage("c2", cpu=1.5, mem=3.0)
        node = s.resource_manager()["nodes"]["n1"]
        assert node["cpu_allocated"] == 2.0
        assert node["cpu_utilization"] == pytest.approx(0.5)
        assert node["cpu_usage"] == pytest.approx(2.0)
        assert node["mem_usage"] == pytest.approx(4.0)

    def test_case_07(self):
        """防超配：节点资源耗尽后调度必须失败，且集群不超配。"""
        s = self.system
        s.add_node("n1", cpu=2, mem=4)
        assert s.schedule_container("c1", cpu_req=1.5, mem_req=2) == "n1"
        assert s.schedule_container("c2", cpu_req=1.0, mem_req=1) is None
        report = s.resource_manager()
        assert report["overcommitted"] is False
        assert report["nodes"]["n1"]["overcommitted"] is False

    # ---- 服务发现：地址跟踪、负载均衡、健康检查 ----

    def test_case_08(self):
        """服务发现：容器 IP 变化后必须能解析到新地址。"""
        s = self.system
        s.add_node("n1", cpu=4, mem=4)
        s.schedule_container("c1", service="lobby", cpu_req=1)
        s.register_service("lobby", "c1", "10.0.0.1:8000")
        assert s.service_discovery("lobby") == "10.0.0.1:8000"
        s.update_container_address("c1", "10.0.0.9:8000")  # 容器重建，IP 变了
        assert s.service_discovery("lobby") == "10.0.0.9:8000"

    def test_case_09(self):
        """负载均衡：多个副本必须轮询分流，不能只用第一个。"""
        s = self.system
        s.add_node("n1", cpu=8, mem=8)
        for i in range(3):
            cid = "c%d" % i
            s.schedule_container(cid, service="game", cpu_req=1)
            s.register_service("game", cid, "10.0.0.%d:8000" % (i + 1))
        picked = {s.service_discovery("game") for _ in range(3)}
        assert picked == {"10.0.0.1:8000", "10.0.0.2:8000", "10.0.0.3:8000"}

    def test_case_10(self):
        """健康检查：挂掉的容器必须摘除，不再接收请求。"""
        s = self.system
        s.add_node("n1", cpu=8, mem=8)
        for i in range(2):
            cid = "c%d" % i
            s.schedule_container(cid, service="game", cpu_req=1)
            s.register_service("game", cid, "10.0.0.%d:8000" % (i + 1))
        s.report_health("c0", False)
        for _ in range(4):
            assert s.service_discovery("game") == "10.0.0.2:8000"
        s.report_health("c1", False)
        assert s.service_discovery("game") is None  # 全部不可用

    # ---- 自动扩缩容：阈值与冷却 ----

    def test_case_11(self):
        """自动扩容：平均 CPU 超过阈值时必须自动加副本。"""
        s = ContainerOrchestrator({"scale_up_threshold": 0.7, "scale_cooldown": 0})
        s.add_node("n1", cpu=16, mem=16)
        s.add_node("n2", cpu=16, mem=16)
        s.schedule_container("r1", service="game", cpu_req=2)
        s.report_usage("r1", cpu=1.0)   # 50%，低于阈值，不扩
        result = s.auto_scaling("game", now=1000.0)
        assert result["action"] == "none"
        s.report_usage("r1", cpu=1.8)   # 90%，超过 70% 阈值
        result = s.auto_scaling("game", now=1001.0)
        assert result["action"] == "scale_out"
        assert result["replicas"] == 2

    def test_case_12(self):
        """自动缩容：利用率低于阈值时缩容，但不能低于最小副本数。"""
        s = ContainerOrchestrator({"scale_down_threshold": 0.2,
                                   "scale_cooldown": 0, "min_replicas": 1})
        s.add_node("n1", cpu=16, mem=16)
        s.schedule_container("r1", service="game", cpu_req=2)
        s.schedule_container("r2", service="game", cpu_req=2)
        s.report_usage("r1", cpu=0.1)
        s.report_usage("r2", cpu=0.1)
        result = s.auto_scaling("game", now=1000.0)
        assert result["action"] == "scale_in"
        assert result["replicas"] == 1
        result = s.auto_scaling("game", now=1001.0)
        assert result["action"] == "none"  # 已到 min_replicas
        assert result["replicas"] == 1

    def test_case_13(self):
        """冷却时间：扩缩容后冷却期内不得再次动作，防止频繁抖动。"""
        s = ContainerOrchestrator({"scale_up_threshold": 0.7,
                                   "scale_cooldown": 60.0})
        for nid in ("n1", "n2", "n3"):
            s.add_node(nid, cpu=16, mem=16)
        s.schedule_container("r1", service="game", cpu_req=2)
        s.report_usage("r1", cpu=1.9)
        first = s.auto_scaling("game", now=1000.0)
        assert first["action"] == "scale_out"
        for cid in ("r1", "game-replica-1"):
            s.report_usage(cid, cpu=1.9)
        second = s.auto_scaling("game", now=1010.0)   # 冷却期内
        assert second["action"] == "none"
        assert second["reason"] == "cooldown"
        third = s.auto_scaling("game", now=1061.0)    # 冷却结束
        assert third["action"] == "scale_out"

    # ---- 滚动更新：分批、版本、回滚 ----

    def test_case_14(self):
        """滚动更新：必须分批进行（不能全部同时重启），且记录版本。"""
        s = self.system
        s.add_node("n1", cpu=16, mem=16)
        for i in range(4):
            s.schedule_container("r%d" % i, service="game", cpu_req=1, version="v1")
        result = s.rolling_update("game", "v2", batch_size=1)
        assert result["status"] == "completed"
        assert len(result["batches"]) == 4          # 一批一个，绝不全量同时重启
        assert all(len(b) == 1 for b in result["batches"])
        assert s.get_version("game") == "v2"
        for i in range(4):
            assert s._containers["r%d" % i]["version"] == "v2"

    def test_case_15(self):
        """回滚：更新失败必须能回退到旧版本。"""
        s = self.system
        s.add_node("n1", cpu=16, mem=16)
        for i in range(3):
            s.schedule_container("r%d" % i, service="game", cpu_req=1, version="v1")
        # 手动回滚
        s.rolling_update("game", "v2", batch_size=1)
        result = s.rollback("game")
        assert result["status"] == "rolled_back"
        assert result["version"] == "v1"
        assert s.get_version("game") == "v1"
        assert all(s._containers["r%d" % i]["version"] == "v1" for i in range(3))
        # 健康检查失败时自动回滚
        bad = s.rolling_update("game", "v3", batch_size=1,
                               health_check=lambda batch: False)
        assert bad["status"] == "rolled_back"
        assert all(s._containers["r%d" % i]["version"] == "v1" for i in range(3))


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
