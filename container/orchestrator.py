"""
游戏内容器编排与调度系统 - 核心模块

功能：
- 容器调度：考虑节点资源余量与负载，支持亲和性与反亲和性
- 资源管理：配额校验、限额管理与使用率监控
- 服务发现：按服务名发现端点，轮询负载均衡，健康检查
- 自动扩缩容：基于 CPU 使用率阈值，带冷却时间
- 滚动更新：分批更新、版本记录与失败回滚
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Config:
    pass


@dataclass
class Node:
    node_id: str
    cpu_capacity: float
    memory_capacity: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class Container:
    container_id: str
    service: str
    node_id: str
    cpu_request: float
    memory_request: float
    cpu_limit: float
    memory_limit: float
    version: str = "v1"
    ip: str = ""
    port: int = 8080
    status: str = "running"
    healthy: bool = True
    labels: Dict[str, str] = field(default_factory=dict)
    cpu_usage: float = 0.0
    memory_usage: float = 0.0


class ContainerOrchestrator:
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or {}
        self.scale_up_threshold = float(self.config.get("scale_up_threshold", 0.8))
        self.scale_down_threshold = float(self.config.get("scale_down_threshold", 0.3))
        self.scale_cooldown = float(self.config.get("scale_cooldown", 60.0))
        self.min_replicas = int(self.config.get("min_replicas", 1))
        self.max_replicas = int(self.config.get("max_replicas", 10))
        self._state = {}
        self._history = []
        self.reset()

    def reset(self):
        self._state = {}
        self._history = []
        self.nodes: Dict[str, Node] = {}
        self.containers: Dict[str, Container] = {}
        self.services: Dict[str, List[str]] = {}
        self._rr_index: Dict[str, int] = {}
        self._last_scale_time: Dict[str, float] = {}
        self._version_history: Dict[str, List[str]] = {}
        self._update_log: List = []
        self._counter = 0
        self._time = 0.0

    def update(self, dt: float):
        self._time += dt

    # ------------------------------------------------------------------
    # 节点与资源管理
    # ------------------------------------------------------------------
    def add_node(self, node_id: str, cpu: float, memory: float,
                 labels: Optional[Dict[str, str]] = None) -> bool:
        if node_id in self.nodes:
            return False
        self.nodes[node_id] = Node(node_id, float(cpu), float(memory), labels or {})
        return True

    def remove_node(self, node_id: str) -> bool:
        if node_id not in self.nodes:
            return False
        for cid in [c.container_id for c in self.containers.values()
                    if c.node_id == node_id]:
            self.remove_container(cid)
        del self.nodes[node_id]
        return True

    def _node_used(self, node_id: str):
        cpu_used = 0.0
        mem_used = 0.0
        for c in self.containers.values():
            if c.node_id == node_id:
                cpu_used += c.cpu_request
                mem_used += c.memory_request
        return cpu_used, mem_used

    def node_has_capacity(self, node_id: str, cpu: float, memory: float) -> bool:
        node = self.nodes.get(node_id)
        if node is None:
            return False
        cpu_used, mem_used = self._node_used(node_id)
        return (cpu_used + cpu <= node.cpu_capacity and
                mem_used + memory <= node.memory_capacity)

    def resource_manager(self) -> Dict:
        """返回各节点资源使用监控报告，并标记超配节点。"""
        report = {}
        overcommitted = []
        for node_id, node in self.nodes.items():
            cpu_used, mem_used = self._node_used(node_id)
            report[node_id] = {
                "cpu_capacity": node.cpu_capacity,
                "cpu_used": cpu_used,
                "cpu_utilization": cpu_used / node.cpu_capacity if node.cpu_capacity else 0.0,
                "memory_capacity": node.memory_capacity,
                "memory_used": mem_used,
                "memory_utilization": mem_used / node.memory_capacity if node.memory_capacity else 0.0,
            }
            if cpu_used > node.cpu_capacity or mem_used > node.memory_capacity:
                overcommitted.append(node_id)
        return {"nodes": report, "overcommitted": overcommitted}

    def report_usage(self, container_id: str, cpu: Optional[float] = None,
                     memory: Optional[float] = None) -> bool:
        c = self.containers.get(container_id)
        if c is None:
            return False
        if cpu is not None:
            c.cpu_usage = float(cpu)
        if memory is not None:
            c.memory_usage = float(memory)
        return True

    def limit_violations(self) -> List[str]:
        """返回实际用量超过限额的容器。"""
        return [cid for cid, c in self.containers.items()
                if c.cpu_usage > c.cpu_limit or c.memory_usage > c.memory_limit]

    # ------------------------------------------------------------------
    # 容器调度
    # ------------------------------------------------------------------
    def schedule_container(self, service: str, cpu: float = 0.5,
                           memory: float = 256.0, version: str = "v1",
                           affinity: Optional[List[str]] = None,
                           anti_affinity: bool = True,
                           labels: Optional[Dict[str, str]] = None,
                           cpu_limit: Optional[float] = None,
                           memory_limit: Optional[float] = None) -> Optional[str]:
        """资源感知 + 负载均衡 + 亲和性/反亲和性调度。成功返回节点ID，失败返回None。"""
        candidates = [n for n in self.nodes.values()
                      if self.node_has_capacity(n.node_id, cpu, memory)]
        if not candidates:
            return None

        def score(node: Node):
            same_service = sum(1 for c in self.containers.values()
                               if c.node_id == node.node_id and c.service == service)
            affinity_hits = 0
            if affinity:
                affinity_hits = sum(1 for c in self.containers.values()
                                    if c.node_id == node.node_id and c.service in affinity)
            cpu_used, mem_used = self._node_used(node.node_id)
            load = (cpu_used / node.cpu_capacity if node.cpu_capacity else 1.0) + \
                   (mem_used / node.memory_capacity if node.memory_capacity else 1.0)
            penalty = same_service if anti_affinity else 0
            return (-affinity_hits, penalty, load, node.node_id)

        best = min(candidates, key=score)
        self._counter += 1
        container_id = "%s-%d" % (service, self._counter)
        ip = "10.0.%d.%d" % (self._counter // 256, self._counter % 256 + 1)
        container = Container(
            container_id=container_id,
            service=service,
            node_id=best.node_id,
            cpu_request=float(cpu),
            memory_request=float(memory),
            cpu_limit=float(cpu_limit) if cpu_limit is not None else float(cpu),
            memory_limit=float(memory_limit) if memory_limit is not None else float(memory),
            version=version,
            ip=ip,
            labels=labels or {},
        )
        self.containers[container_id] = container
        self.services.setdefault(service, []).append(container_id)
        return best.node_id

    def remove_container(self, container_id: str) -> bool:
        c = self.containers.pop(container_id, None)
        if c is None:
            return False
        if c.service in self.services and container_id in self.services[c.service]:
            self.services[c.service].remove(container_id)
        return True

    def containers_of(self, service: str) -> List[Container]:
        return [self.containers[cid] for cid in self.services.get(service, [])
                if cid in self.containers]

    # ------------------------------------------------------------------
    # 服务发现
    # ------------------------------------------------------------------
    def set_health(self, container_id: str, healthy: bool) -> bool:
        c = self.containers.get(container_id)
        if c is None:
            return False
        c.healthy = bool(healthy)
        return True

    def endpoints(self, service: str) -> List[str]:
        """按服务名实时发现健康端点，容器 IP 变化不影响查找。"""
        result = []
        for cid in self.services.get(service, []):
            c = self.containers.get(cid)
            if c is not None and c.status == "running" and c.healthy:
                result.append("%s:%d" % (c.ip, c.port))
        return result

    def discover(self, service: str) -> Optional[str]:
        """轮询负载均衡地返回一个健康端点。"""
        eps = self.endpoints(service)
        if not eps:
            return None
        idx = self._rr_index.get(service, 0)
        self._rr_index[service] = idx + 1
        return eps[idx % len(eps)]

    # ------------------------------------------------------------------
    # 自动扩缩容
    # ------------------------------------------------------------------
    def auto_scale(self, service: str, now: Optional[float] = None) -> Optional[str]:
        """基于平均 CPU 使用率阈值扩缩容，带冷却时间。返回 "scale_up"/"scale_down"/None。"""
        now = self._time if now is None else now
        replicas = self.containers_of(service)
        if not replicas:
            return None
        last = self._last_scale_time.get(service, float("-inf"))
        if now - last < self.scale_cooldown:
            return None
        utils = [(c.cpu_usage / c.cpu_request) if c.cpu_request > 0 else 0.0
                 for c in replicas]
        avg = sum(utils) / len(utils)
        if avg >= self.scale_up_threshold and len(replicas) < self.max_replicas:
            proto = replicas[0]
            node = self.schedule_container(
                service, cpu=proto.cpu_request, memory=proto.memory_request,
                version=proto.version, cpu_limit=proto.cpu_limit,
                memory_limit=proto.memory_limit)
            if node is None:
                return None
            self._last_scale_time[service] = now
            return "scale_up"
        if avg <= self.scale_down_threshold and len(replicas) > self.min_replicas:
            self.remove_container(replicas[-1].container_id)
            self._last_scale_time[service] = now
            return "scale_down"
        return None

    # ------------------------------------------------------------------
    # 滚动更新与回滚
    # ------------------------------------------------------------------
    def rolling_update(self, service: str, new_version: str,
                       batch_size: int = 1) -> bool:
        """分批滚动更新，记录版本历史，避免所有容器同时重启。"""
        cids = list(self.services.get(service, []))
        if not cids or batch_size < 1:
            return False
        current = self.service_version(service)
        if current is not None:
            history = self._version_history.setdefault(service, [])
            if not history or history[-1] != current:
                history.append(current)
        batches = [cids[i:i + batch_size] for i in range(0, len(cids), batch_size)]
        for batch_index, batch in enumerate(batches):
            for cid in batch:
                self.containers[cid].version = new_version
            self._update_log.append((batch_index, list(batch), new_version))
        return True

    def rollback(self, service: str) -> Optional[str]:
        """回滚到上一个版本，返回回滚后的版本。"""
        history = self._version_history.get(service, [])
        if not history:
            return None
        previous = history.pop()
        for cid in self.services.get(service, []):
            if cid in self.containers:
                self.containers[cid].version = previous
        return previous

    def service_version(self, service: str) -> Optional[str]:
        replicas = self.containers_of(service)
        return replicas[0].version if replicas else None

    def version_history(self, service: str) -> List[str]:
        return list(self._version_history.get(service, []))

    @property
    def update_log(self) -> List:
        return list(self._update_log)
