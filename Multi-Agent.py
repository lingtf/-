"""
多 Agent 协同运营自动化系统
使用 asyncio 实现多智能体：Coordinator, Worker, Monitor, Reporter
所有依赖均为 Python 标准库
"""

import asyncio
import logging
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional

# ---------- 日志配置 ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("OPS-Agents")


# ---------- 基础数据结构 ----------
class TaskType(Enum):
    DATA_PROCESS = "data_process"
    SYSTEM_CHECK = "system_check"
    ALERT = "alert"
    REPORT_GEN = "report_gen"


class TaskStatus(Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Task:
    id: str
    type: TaskType
    payload: Dict[str, Any]
    status: TaskStatus = TaskStatus.PENDING
    assigned_worker: Optional[str] = None
    result: Optional[Any] = None
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class Message:
    """Agent 间消息"""
    sender: str
    recipient: Optional[str]  # None 表示广播
    msg_type: str  # task, heartbeat, status, result, cmd
    content: Any


# ---------- 消息总线 ----------
class MessageBus:
    """异步消息总线，每个 Agent 有自己的队列"""

    def __init__(self):
        self._queues: Dict[str, asyncio.Queue] = {}

    def register(self, agent_name: str):
        self._queues[agent_name] = asyncio.Queue()
        logger.debug(f"消息总线注册: {agent_name}")

    async def send(self, msg: Message):
        if msg.recipient:
            if msg.recipient in self._queues:
                await self._queues[msg.recipient].put(msg)
        else:  # 广播
            for name, q in self._queues.items():
                if name != msg.sender:
                    await q.put(msg)

    async def receive(self, agent_name: str) -> Message:
        return await self._queues[agent_name].get()


# ---------- Agent 基类 ----------
class BaseAgent:
    def __init__(self, name: str, bus: MessageBus):
        self.name = name
        self.bus = bus
        self.bus.register(name)
        self._running = False

    async def send(self, recipient: Optional[str], msg_type: str, content: Any):
        await self.bus.send(Message(self.name, recipient, msg_type, content))

    async def run(self):
        """Agent 主循环，子类需实现 handle_message"""
        self._running = True
        logger.info(f"{self.name} 启动")
        while self._running:
            try:
                msg = await asyncio.wait_for(self.bus.receive(self.name), timeout=1.0)
                await self.handle_message(msg)
            except asyncio.TimeoutError:
                # 允许空闲时执行周期任务
                await self.idle_tick()
            except Exception as e:
                logger.error(f"{self.name} 处理消息出错: {e}")
        logger.info(f"{self.name} 已停止")

    async def handle_message(self, msg: Message):
        raise NotImplementedError

    async def idle_tick(self):
        """空闲时的周期操作"""
        pass

    def stop(self):
        self._running = False


# ---------- CoordinatorAgent ----------
class CoordinatorAgent(BaseAgent):
    """任务协调器：接收新任务，指派给 Worker"""

    def __init__(self, bus: MessageBus, workers: List[str]):
        super().__init__("Coordinator", bus)
        self.workers = workers
        self.task_counter = 0
        self.pending_tasks: Dict[str, Task] = {}

    async def handle_message(self, msg: Message):
        if msg.msg_type == "new_task":
            task: Task = msg.content
            self.pending_tasks[task.id] = task
            # 简单轮询指派
            worker = self.workers[self.task_counter % len(self.workers)]
            self.task_counter += 1
            task.status = TaskStatus.ASSIGNED
            task.assigned_worker = worker
            logger.info(f"任务 {task.id}({task.type.value}) 指派给 {worker}")
            await self.send(worker, "task_assign", task)

        elif msg.msg_type == "task_result":
            task: Task = msg.content
            if task.id in self.pending_tasks:
                self.pending_tasks[task.id].status = task.status
                self.pending_tasks[task.id].result = task.result
                logger.info(
                    f"任务 {task.id} 完成，状态: {task.status.value}，结果: {task.result}"
                )
            # 将结果发布给监控和报告 Agent
            await self.send(None, "task_status", task)

    async def idle_tick(self):
        pass  # 可在此添加主动调度逻辑


# ---------- WorkerAgent ----------
class WorkerAgent(BaseAgent):
    """工作执行器：处理分配的任务，支持多种任务类型"""

    def __init__(self, name: str, bus: MessageBus):
        super().__init__(name, bus)
        self.current_task: Optional[Task] = None

    async def handle_message(self, msg: Message):
        if msg.msg_type == "task_assign":
            task: Task = msg.content
            self.current_task = task
            task.status = TaskStatus.RUNNING
            logger.info(f"{self.name} 开始执行任务 {task.id}")
            # 模拟任务执行
            try:
                result = await self.execute_task(task)
                task.status = TaskStatus.COMPLETED
                task.result = result
            except Exception as e:
                logger.error(f"{self.name} 执行任务 {task.id} 失败: {e}")
                task.status = TaskStatus.FAILED
                task.result = str(e)
            # 返回结果给 Coordinator
            await self.send("Coordinator", "task_result", task)
            self.current_task = None

    async def execute_task(self, task: Task) -> Any:
        """根据不同任务类型执行相应逻辑（模拟）"""
        await asyncio.sleep(random.uniform(0.5, 2.0))  # 模拟耗时
        if task.type == TaskType.DATA_PROCESS:
            return f"已处理 {task.payload.get('records', 0)} 条记录"
        elif task.type == TaskType.SYSTEM_CHECK:
            status = "正常" if random.random() > 0.2 else "异常"
            return f"系统检查完成: {status}"
        elif task.type == TaskType.ALERT:
            level = task.payload.get("level", "INFO")
            message = task.payload.get("message", "无消息")
            logger.warning(f"[ALERT][{level}] {message}")
            return f"警报已发送: {message}"
        elif task.type == TaskType.REPORT_GEN:
            return "报告已生成: 运营数据汇总"
        else:
            return "未知任务类型"

    async def idle_tick(self):
        # 定期发送心跳
        await self.send("Monitor", "heartbeat", {"agent": self.name, "time": datetime.now(), "busy": self.current_task is not None})


# ---------- MonitorAgent ----------
class MonitorAgent(BaseAgent):
    """监控器：收集心跳、任务状态，检测异常"""

    def __init__(self, bus: MessageBus):
        super().__init__("Monitor", bus)
        self.agent_status: Dict[str, Dict] = {}  # 最近心跳
        self.task_log: List[Task] = []

    async def handle_message(self, msg: Message):
        if msg.msg_type == "heartbeat":
            data = msg.content
            self.agent_status[data["agent"]] = data
            logger.debug(f"收到心跳: {data['agent']} busy={data['busy']}")
        elif msg.msg_type == "task_status":
            task: Task = msg.content
            self.task_log.append(task)
            if task.status == TaskStatus.FAILED:
                logger.error(f"[监控] 任务失败: {task.id} 原因: {task.result}")
                # 可触发自动重试或告警
                await self.send("Coordinator", "new_task", Task(
                    id=f"retry-{task.id}",
                    type=TaskType.ALERT,
                    payload={"level": "ERROR", "message": f"任务失败自动告警: {task.id}"},
                ))

    async def idle_tick(self):
        # 定期检查 Agent 超时（10秒无心跳）
        now = datetime.now()
        for agent, status in list(self.agent_status.items()):
            if now - status["time"] > timedelta(seconds=10):
                logger.warning(f"[监控] Agent {agent} 心跳超时！")


# ---------- ReporterAgent ----------
class ReporterAgent(BaseAgent):
    """报告生成器：定期输出运营报告"""

    def __init__(self, bus: MessageBus):
        super().__init__("Reporter", bus)
        self.task_stats = {"completed": 0, "failed": 0, "pending": 0}

    async def handle_message(self, msg: Message):
        if msg.msg_type == "task_status":
            task: Task = msg.content
            if task.status == TaskStatus.COMPLETED:
                self.task_stats["completed"] += 1
            elif task.status == TaskStatus.FAILED:
                self.task_stats["failed"] += 1

    async def idle_tick(self):
        # 每5秒输出一次报告
        await asyncio.sleep(5)
        # 实际上需要累积时间，这里简单演示每次 idle 输出（因为 timeout 1秒，不适合精确周期）
        # 改用独立定时发送方式；简单起见这里不输出以免刷屏，改为接收到任务时输出摘要
        pass

    async def generate_report(self):
        # 可以在 main 中周期调用
        logger.info(
            f"[运营报告] 已完成: {self.task_stats['completed']}, "
            f"失败: {self.task_stats['failed']}"
        )


# ---------- 系统主程序 ----------
async def main():
    bus = MessageBus()

    # 注册所有 Agent
    workers = [WorkerAgent(f"Worker-{i}", bus) for i in range(3)]
    coordinator = CoordinatorAgent(bus, [w.name for w in workers])
    monitor = MonitorAgent(bus)
    reporter = ReporterAgent(bus)

    agents = [coordinator, monitor, reporter] + workers

    # 启动所有 Agent
    agent_tasks = [asyncio.create_task(agent.run()) for agent in agents]

    # 模拟提交一些任务
    logger.info("=== 运营自动化系统启动 ===")
    await asyncio.sleep(1)

    sample_tasks = [
        Task(id="T001", type=TaskType.DATA_PROCESS, payload={"records": 1500}),
        Task(id="T002", type=TaskType.SYSTEM_CHECK, payload={"target": "server-1"}),
        Task(id="T003", type=TaskType.ALERT, payload={"level": "WARN", "message": "磁盘使用率超过80%"}),
        Task(id="T004", type=TaskType.DATA_PROCESS, payload={"records": 300}),
        Task(id="T005", type=TaskType.REPORT_GEN, payload={}),
        Task(id="T006", type=TaskType.SYSTEM_CHECK, payload={"target": "db-master"}),
    ]

    for task in sample_tasks:
        await coordinator.send("Coordinator", "new_task", task)
        await asyncio.sleep(random.uniform(0.2, 0.8))  # 模拟任务间隔

    # 运行30秒后停止（实际场景可长期运行）
    await asyncio.sleep(15)

    # 输出最终报告
    await reporter.generate_report()

    # 停止所有 Agent
    for agent in agents:
        agent.stop()

    await asyncio.gather(*agent_tasks, return_exceptions=True)
    logger.info("=== 系统已关闭 ===")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("用户中断")
