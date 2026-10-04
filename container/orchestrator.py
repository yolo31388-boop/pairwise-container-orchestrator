"""
游戏内容器编排与调度系统 - 核心模块

功能：
- 容器调度：感知资源与负载，支持亲和性 / 反亲和性
- 资源管理：限额、监控、防止超配
- 服务发现：负载均衡、健康检查、地址变更跟踪
- 自动扩缩容：阈值触发、冷却时间
- 滚动更新：分批更新、版本记录、失败回滚
"""
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional
import time


@dataclass
class Config:
    pass


class ContainerOrchestrator:
    DEFAULTS = {
        "scale_up_threshold": 0.8,    # 平均 CPU 利用率超过该值则扩容
        "scale_down_threshold": 0.2,  # 平均 CPU 利用率低于该值则缩容
        "scale_cooldown": 60.0,       # 两次扩缩容之间的冷却秒数
        "min_replicas": 1,
        "max_replicas": 10,
    }

    def __init__(self, config: Optional[Dict] = None):
        self.config = dict(self.DEFAULTS)
        if config:
            self.config.update(config)
        self._state = {}
        self._history = []
        self._nodes: Dict[str, Dict] = {}
        self._containers: Dict[str, Dict] = {}
        self._services: Dict[str, List[str]] = {}
        self._versions: Dict[str, List[str]] = {}
        self._rr_index: Dict[str, int] = {}
        self._last_scale_time: Dict[str, float] = {}
        self._replica_counter: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # 节点管理
    # ------------------------------------------------------------------
    def add_node(self, node_id: str, cpu: float, mem: float):
        self._nodes[node_id] = {
            "cpu_capacity": float(cpu),
            "mem_capacity": float(mem),
            "containers": [],
        }

    # ------------------------------------------------------------------
    # 容器调度：资源 + 负载感知，亲和性 / 反亲和性
    # ------------------------------------------------------------------
    def schedule_container(
        self,
        container_id: Optional[str] = None,
        image: str = "game-server",
        service: Optional[str] = None,
        version: str = "v1",
        cpu_req: float = 0.0,
        mem_req: float = 0.0,
        affinity: Optional[List[str]] = None,
        anti_affinity = None,
        address: Optional[str] = None,
    ) -> Optional[str]:
        """调度容器到合适节点，返回节点 id；资源不足或约束无法满足时返回 None。"""
        service = service or image
        if container_id is None:
            n = self._replica_counter.get(service, 0) + 1
            self._replica_counter[service] = n
            container_id = "%s-replica-%d" % (service, n)

        anti_services = set()
        if anti_affinity is True:
            anti_services.add(service)
        elif isinstance(anti_affinity, (list, tuple, set)):
            anti_services.update(anti_affinity)

        # 1) 资源硬约束：节点剩余资源必须足够，禁止超配
        candidates = []
        for node_id, node in self._nodes.items():
            if self._node_free_cpu(node_id) >= cpu_req and \
                    self._node_free_mem(node_id) >= mem_req:
                candidates.append(node_id)

        # 2) 反亲和性：副本必须分散，同服务容器不得同机
        if anti_services:
            candidates = [
                nid for nid in candidates
                if not any(
                    self._containers[c]["service"] in anti_services
                    for c in self._nodes[nid]["containers"]
                )
            ]

        if not candidates:
            return None

        # 3) 亲和性：相关容器尽量调度到一起
        if affinity:
            affinity_set = set(affinity)
            preferred = [
                nid for nid in candidates
                if any(
                    self._containers[c]["service"] in affinity_set
                    for c in self._nodes[nid]["containers"]
                )
            ]
            if preferred:
                candidates = preferred

        # 4) 负载感知：选择调度后负载最低的节点（而非轮询）
        best = min(candidates, key=lambda nid: self._node_load_score(nid, cpu_req, mem_req))

        self._containers[container_id] = {
            "id": container_id,
            "image": image,
            "service": service,
            "version": version,
            "node": best,
            "cpu_req": float(cpu_req),
            "mem_req": float(mem_req),
            "cpu_limit": None,
            "mem_limit": None,
            "cpu_usage": 0.0,
            "mem_usage": 0.0,
            "address": address,
            "healthy": True,
        }
        self._nodes[best]["containers"].append(container_id)
        self._services.setdefault(service, []).append(container_id)
        self._versions.setdefault(service, [version])
        return best

    def _node_free_cpu(self, node_id: str) -> float:
        node = self._nodes[node_id]
        used = sum(self._containers[c]["cpu_req"] for c in node["containers"])
        return node["cpu_capacity"] - used

    def _node_free_mem(self, node_id: str) -> float:
        node = self._nodes[node_id]
        used = sum(self._containers[c]["mem_req"] for c in node["containers"])
        return node["mem_capacity"] - used

    def _node_load_score(self, node_id: str, cpu_req: float, mem_req: float) -> float:
        """调度后的预期利用率，越高越忙。"""
        node = self._nodes[node_id]
        cpu_util = 0.0
        mem_util = 0.0
        if node["cpu_capacity"] > 0:
            cpu_util = (node["cpu_capacity"] - self._node_free_cpu(node_id) + cpu_req) \
                / node["cpu_capacity"]
        if node["mem_capacity"] > 0:
            mem_util = (node["mem_capacity"] - self._node_free_mem(node_id) + mem_req) \
                / node["mem_capacity"]
        return max(cpu_util, mem_util)

    # ------------------------------------------------------------------
    # 资源管理：限额 + 监控 + 防超配
    # ------------------------------------------------------------------
    def set_resource_limit(self, container_id: str,
                           cpu: Optional[float] = None,
                           mem: Optional[float] = None):
        c = self._containers[container_id]
        if cpu is not None:
            c["cpu_limit"] = float(cpu)
        if mem is not None:
            c["mem_limit"] = float(mem)

    def report_usage(self, container_id: str,
                     cpu: Optional[float] = None,
                     mem: Optional[float] = None):
        c = self._containers[container_id]
        if cpu is not None:
            c["cpu_usage"] = float(cpu)
        if mem is not None:
            c["mem_usage"] = float(mem)

    def resource_manager(self) -> Dict:
        """返回各节点资源分配/实际使用、容器限额与超限告警。"""
        nodes = {}
        for node_id, node in self._nodes.items():
            cpu_alloc = sum(self._containers[c]["cpu_req"] for c in node["containers"])
            mem_alloc = sum(self._containers[c]["mem_req"] for c in node["containers"])
            cpu_used = sum(self._containers[c]["cpu_usage"] for c in node["containers"])
            mem_used = sum(self._containers[c]["mem_usage"] for c in node["containers"])
            nodes[node_id] = {
                "cpu_capacity": node["cpu_capacity"],
                "mem_capacity": node["mem_capacity"],
                "cpu_allocated": cpu_alloc,
                "mem_allocated": mem_alloc,
                "cpu_usage": cpu_used,
                "mem_usage": mem_used,
                "cpu_utilization": cpu_alloc / node["cpu_capacity"] if node["cpu_capacity"] else 0.0,
                "mem_utilization": mem_alloc / node["mem_capacity"] if node["mem_capacity"] else 0.0,
                "overcommitted": cpu_alloc > node["cpu_capacity"]
                    or mem_alloc > node["mem_capacity"],
                "containers": list(node["containers"]),
            }

        limits = {}
        violations = []
        for cid, c in self._containers.items():
            limits[cid] = {"cpu_limit": c["cpu_limit"], "mem_limit": c["mem_limit"]}
            if c["cpu_limit"] is not None and c["cpu_usage"] > c["cpu_limit"]:
                violations.append(cid)
            elif c["mem_limit"] is not None and c["mem_usage"] > c["mem_limit"]:
                violations.append(cid)

        return {
            "nodes": nodes,
            "limits": limits,
            "violations": violations,
            "overcommitted": any(n["overcommitted"] for n in nodes.values()),
        }

    # ------------------------------------------------------------------
    # 服务发现：地址跟踪 + 负载均衡 + 健康检查
    # ------------------------------------------------------------------
    def register_service(self, service: str, container_id: str, address: str):
        c = self._containers[container_id]
        c["service"] = service
        c["address"] = address
        if container_id not in self._services.setdefault(service, []):
            self._services[service].append(container_id)

    def update_container_address(self, container_id: str, address: str):
        """容器 IP 变化后更新注册表，服务发现立即可见。"""
        self._containers[container_id]["address"] = address

    def report_health(self, container_id: str, healthy: bool):
        self._containers[container_id]["healthy"] = bool(healthy)

    def get_endpoints(self, service: str, healthy_only: bool = True) -> List[str]:
        endpoints = []
        for cid in self._services.get(service, []):
            c = self._containers[cid]
            if c["address"] is None:
                continue
            if healthy_only and not c["healthy"]:
                continue
            endpoints.append(c["address"])
        return endpoints

    def service_discovery(self, service: str) -> Optional[str]:
        """在健康副本间轮询负载均衡，返回一个可用地址；无健康副本返回 None。"""
        endpoints = self.get_endpoints(service, healthy_only=True)
        if not endpoints:
            return None
        idx = self._rr_index.get(service, 0)
        self._rr_index[service] = idx + 1
        return endpoints[idx % len(endpoints)]

    # ------------------------------------------------------------------
    # 自动扩缩容：阈值触发 + 冷却时间
    # ------------------------------------------------------------------
    def auto_scaling(self, service: str, now: Optional[float] = None) -> Dict:
        now = time.time() if now is None else now
        replicas = list(self._services.get(service, []))
        if not replicas:
            return {"action": "none", "replicas": 0, "reason": "no_such_service"}

        # 冷却期内的频繁扩缩容直接拒绝
        last = self._last_scale_time.get(service)
        cooldown = self.config["scale_cooldown"]
        if last is not None and now - last < cooldown:
            return {"action": "none", "replicas": len(replicas), "reason": "cooldown"}

        utils = [self._container_utilization(cid) for cid in replicas]
        avg_util = sum(utils) / len(utils) if utils else 0.0

        if avg_util > self.config["scale_up_threshold"]:
            if len(replicas) >= self.config["max_replicas"]:
                return {"action": "none", "replicas": len(replicas), "reason": "max_replicas"}
            template = self._containers[replicas[0]]
            node = self.schedule_container(
                image=template["image"],
                service=service,
                version=template["version"],
                cpu_req=template["cpu_req"],
                mem_req=template["mem_req"],
                anti_affinity=True,
            )
            if node is None:
                return {"action": "none", "replicas": len(replicas), "reason": "no_capacity"}
            self._last_scale_time[service] = now
            return {"action": "scale_out", "replicas": len(replicas) + 1,
                    "avg_utilization": avg_util}

        if avg_util < self.config["scale_down_threshold"]:
            if len(replicas) <= self.config["min_replicas"]:
                return {"action": "none", "replicas": len(replicas), "reason": "min_replicas"}
            victim = replicas[-1]
            self._remove_container(victim)
            self._last_scale_time[service] = now
            return {"action": "scale_in", "replicas": len(replicas) - 1,
                    "avg_utilization": avg_util}

        return {"action": "none", "replicas": len(replicas),
                "reason": "within_thresholds", "avg_utilization": avg_util}

    def _container_utilization(self, container_id: str) -> float:
        c = self._containers[container_id]
        base = c["cpu_limit"] if c["cpu_limit"] else c["cpu_req"]
        if not base:
            return 0.0
        return c["cpu_usage"] / base

    def _remove_container(self, container_id: str):
        c = self._containers.pop(container_id)
        self._nodes[c["node"]]["containers"].remove(container_id)
        if container_id in self._services.get(c["service"], []):
            self._services[c["service"]].remove(container_id)

    # ------------------------------------------------------------------
    # 滚动更新：分批 + 版本记录 + 回滚
    # ------------------------------------------------------------------
    def rolling_update(
        self,
        service: str,
        new_version: str,
        batch_size: int = 1,
        health_check: Optional[Callable[[List[str]], bool]] = None,
    ) -> Dict:
        replicas = list(self._services.get(service, []))
        if not replicas:
            return {"status": "failed", "reason": "no_such_service", "batches": []}

        old_version = self._containers[replicas[0]]["version"]
        batch_size = max(1, min(batch_size, len(replicas)))
        batches = [replicas[i:i + batch_size]
                   for i in range(0, len(replicas), batch_size)]

        done = []
        for batch in batches:
            for cid in batch:
                self._containers[cid]["version"] = new_version
            done.extend(batch)
            # 每批更新后做健康检查，失败立即回滚已更新的批次
            if health_check is not None and not health_check(batch):
                for cid in done:
                    self._containers[cid]["version"] = old_version
                return {
                    "status": "rolled_back",
                    "version": old_version,
                    "failed_version": new_version,
                    "batches": batches,
                }

        self._versions.setdefault(service, [old_version])
        if self._versions[service][-1] != new_version:
            self._versions[service].append(new_version)
        self._history.append({
            "type": "rolling_update",
            "service": service,
            "from": old_version,
            "to": new_version,
        })
        return {"status": "completed", "version": new_version, "batches": batches}

    def rollback(self, service: str) -> Dict:
        """回滚到上一个版本。"""
        history = self._versions.get(service, [])
        if len(history) < 2:
            return {"status": "failed", "reason": "no_previous_version"}
        history.pop()  # 丢弃当前版本
        previous = history[-1]
        for cid in self._services.get(service, []):
            self._containers[cid]["version"] = previous
        self._history.append({
            "type": "rollback",
            "service": service,
            "to": previous,
        })
        return {"status": "rolled_back", "version": previous}

    def get_version(self, service: str) -> Optional[str]:
        history = self._versions.get(service)
        return history[-1] if history else None

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    def update(self, dt: float):
        pass

    def reset(self):
        self._state = {}
        self._history = []
        self._nodes = {}
        self._containers = {}
        self._services = {}
        self._versions = {}
        self._rr_index = {}
        self._last_scale_time = {}
        self._replica_counter = {}
