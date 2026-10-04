"""
游戏内容器编排与调度系统 - 验收测试
运行方式：python -m pytest tests/test_container_orchestrator.py -q
共 15 个测试用例

调度(01-04) / 资源管理(05-07) / 服务发现(08-10) /
自动扩缩容(11-12) / 滚动更新(13-15)
"""
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from container.orchestrator import ContainerOrchestrator


class TestContainerOrchestrator:
    def setup_method(self):
        self.system = ContainerOrchestrator()

    # ---------- 容器调度 ----------
    def test_case_01(self):
        """调度必须考虑资源余量，资源不足的节点不能被选中"""
        self.system.add_node("n1", cpu=1.0, memory=1024)
        self.system.add_node("n2", cpu=4.0, memory=4096)

        first = self.system.schedule_container("game", cpu=0.8, memory=512)
        assert first == "n1"

        second = self.system.schedule_container("game", cpu=0.9, memory=512)
        assert second == "n2"

        assert self.system.schedule_container("game", cpu=8.0, memory=8192) is None

    def test_case_02(self):
        """调度必须按负载均衡，把容器分散到最空闲的节点"""
        self.system.add_node("n1", cpu=4.0, memory=4096)
        self.system.add_node("n2", cpu=4.0, memory=4096)
        self.system.add_node("n3", cpu=4.0, memory=4096)

        nodes = {self.system.schedule_container("web", cpu=1.0, memory=512,
                                                anti_affinity=False)
                 for _ in range(3)}
        assert nodes == {"n1", "n2", "n3"}

    def test_case_03(self):
        """亲和性：相关容器应调度到同一台机器"""
        self.system.add_node("n1", cpu=8.0, memory=8192)
        self.system.add_node("n2", cpu=8.0, memory=8192)

        assert self.system.schedule_container("db", cpu=1.0, memory=1024) == "n1"

        cache_nodes = [
            self.system.schedule_container("cache", cpu=0.5, memory=256,
                                           affinity=["db"])
            for _ in range(2)
        ]
        assert cache_nodes == ["n1", "n1"]

    def test_case_04(self):
        """反亲和性：同一服务的多个副本不能调度到同一台机器"""
        self.system.add_node("n1", cpu=8.0, memory=8192)
        self.system.add_node("n2", cpu=8.0, memory=8192)

        replicas = [self.system.schedule_container("web", cpu=1.0, memory=512)
                    for _ in range(2)]
        assert replicas[0] != replicas[1]
        assert len(self.system.containers_of("web")) == 2

    # ---------- 资源管理 ----------
    def test_case_05(self):
        """资源配额：超配的调度请求必须被拒绝，节点使用量不得超过容量"""
        self.system.add_node("n1", cpu=2.0, memory=2048)

        assert self.system.schedule_container("a", cpu=1.2, memory=1024) == "n1"
        assert self.system.schedule_container("b", cpu=0.8, memory=1024) == "n1"
        assert self.system.schedule_container("c", cpu=0.1, memory=100) is None

        report = self.system.resource_manager()["nodes"]["n1"]
        assert report["cpu_used"] <= report["cpu_capacity"]
        assert report["memory_used"] <= report["memory_capacity"]
        assert self.system.resource_manager()["overcommitted"] == []

    def test_case_06(self):
        """资源限额：容器实际用量超过限额必须能被发现"""
        self.system.add_node("n1", cpu=8.0, memory=8192)
        cid = self.system.services["job"][0] if False else None
        node = self.system.schedule_container("job", cpu=1.0, memory=1024,
                                              cpu_limit=1.0, memory_limit=1024)
        assert node == "n1"
        cid = self.system.services["job"][0]

        self.system.report_usage(cid, cpu=1.5, memory=1200)
        assert cid in self.system.limit_violations()

        self.system.report_usage(cid, cpu=0.9, memory=900)
        assert cid not in self.system.limit_violations()

    def test_case_07(self):
        """资源监控：必须能查询每个节点的 CPU/内存使用率"""
        self.system.add_node("n1", cpu=4.0, memory=4096)
        self.system.schedule_container("a", cpu=2.0, memory=1024,
                                       anti_affinity=False)
        self.system.schedule_container("b", cpu=1.0, memory=2048,
                                       anti_affinity=False)

        report = self.system.resource_manager()["nodes"]["n1"]
        assert report["cpu_used"] == 3.0
        assert report["cpu_utilization"] == pytest.approx(0.75)
        assert report["memory_used"] == 3072
        assert report["memory_utilization"] == pytest.approx(0.75)

    # ---------- 服务发现 ----------
    def test_case_08(self):
        """服务发现：按服务名查找端点，容器 IP 变化后仍能找到"""
        self.system.add_node("n1", cpu=4.0, memory=4096)
        self.system.schedule_container("web", cpu=1.0, memory=512)
        cid = self.system.services["web"][0]

        endpoint = self.system.discover("web")
        assert endpoint is not None

        self.system.containers[cid].ip = "10.9.9.9"
        assert self.system.discover("web") == "10.9.9.9:8080"

    def test_case_09(self):
        """服务发现负载均衡：多个副本应轮询返回，不能只用第一个"""
        self.system.add_node("n1", cpu=8.0, memory=8192)
        self.system.add_node("n2", cpu=8.0, memory=8192)
        self.system.add_node("n3", cpu=8.0, memory=8192)
        for _ in range(3):
            self.system.schedule_container("web", cpu=1.0, memory=512)

        first_three = [self.system.discover("web") for _ in range(3)]
        assert len(set(first_three)) == 3
        fourth = self.system.discover("web")
        assert fourth == first_three[0]

    def test_case_10(self):
        """健康检查：挂掉的容器不能再接收请求，全部不健康时返回 None"""
        self.system.add_node("n1", cpu=8.0, memory=8192)
        self.system.add_node("n2", cpu=8.0, memory=8192)
        for _ in range(2):
            self.system.schedule_container("web", cpu=1.0, memory=512)

        bad = self.system.services["web"][0]
        good_ep = "%s:8080" % self.system.containers[
            self.system.services["web"][1]].ip
        self.system.set_health(bad, False)

        for _ in range(4):
            assert self.system.discover("web") == good_ep

        self.system.set_health(self.system.services["web"][1], False)
        assert self.system.discover("web") is None

    # ---------- 自动扩缩容 ----------
    def test_case_11(self):
        """自动扩容：CPU 超过阈值时自动增加副本"""
        self.system.add_node("n1", cpu=8.0, memory=8192)
        self.system.add_node("n2", cpu=8.0, memory=8192)
        self.system.schedule_container("web", cpu=1.0, memory=512)
        cid = self.system.services["web"][0]
        self.system.report_usage(cid, cpu=0.95)

        assert self.system.auto_scale("web") == "scale_up"
        assert len(self.system.containers_of("web")) == 2

    def test_case_12(self):
        """缩容冷却：冷却期内不能重复扩缩容，冷却结束后才能继续缩容"""
        self.system.add_node("n1", cpu=8.0, memory=8192)
        self.system.add_node("n2", cpu=8.0, memory=8192)
        self.system.add_node("n3", cpu=8.0, memory=8192)
        for _ in range(3):
            self.system.schedule_container("web", cpu=1.0, memory=512)
        for cid in self.system.services["web"]:
            self.system.report_usage(cid, cpu=0.1)

        assert self.system.auto_scale("web", now=0) == "scale_down"
        assert len(self.system.containers_of("web")) == 2
        assert self.system.auto_scale("web", now=10) is None

        self.system.update(61)
        assert self.system.auto_scale("web") == "scale_down"
        assert len(self.system.containers_of("web")) == 1

        assert self.system.auto_scale("web") is None

    # ---------- 滚动更新 ----------
    def test_case_13(self):
        """滚动更新必须分批重启，不能所有容器同时重启"""
        self.system.add_node("n1", cpu=8.0, memory=8192)
        self.system.add_node("n2", cpu=8.0, memory=8192)
        for _ in range(4):
            self.system.schedule_container("web", cpu=1.0, memory=512)

        assert self.system.rolling_update("web", "v2", batch_size=1) is True
        log = self.system.update_log
        assert len(log) == 4
        assert max(len(batch) for _, batch, _ in log) == 1
        assert log[0][0] == 0 and log[-1][0] == 3

    def test_case_14(self):
        """版本管理：更新后能查到新版本，且历史中保留旧版本"""
        self.system.add_node("n1", cpu=8.0, memory=8192)
        self.system.add_node("n2", cpu=8.0, memory=8192)
        for _ in range(2):
            self.system.schedule_container("web", cpu=1.0, memory=512)

        assert self.system.service_version("web") == "v1"
        self.system.rolling_update("web", "v2", batch_size=2)

        assert self.system.service_version("web") == "v2"
        assert {c.version for c in self.system.containers_of("web")} == {"v2"}
        assert self.system.version_history("web") == ["v1"]

    def test_case_15(self):
        """更新回滚：回滚后所有容器恢复到上一个版本"""
        self.system.add_node("n1", cpu=8.0, memory=8192)
        self.system.add_node("n2", cpu=8.0, memory=8192)
        for _ in range(2):
            self.system.schedule_container("web", cpu=1.0, memory=512)

        self.system.rolling_update("web", "v2", batch_size=1)
        assert self.system.service_version("web") == "v2"

        assert self.system.rollback("web") == "v1"
        assert self.system.service_version("web") == "v1"
        assert {c.version for c in self.system.containers_of("web")} == {"v1"}
        assert self.system.version_history("web") == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
