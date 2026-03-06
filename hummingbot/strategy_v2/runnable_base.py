import asyncio
import logging
import time
from abc import ABC
from typing import Dict

from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger
from hummingbot.strategy_v2.models.base import RunnableStatus


class RunnableBase(ABC):
    """
    Base class for smart components in the Hummingbot application.
    This class provides a basic structure for components that need to perform tasks at regular intervals.
    """
    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, update_interval: float = 0.5):
        """
        Initialize a new instance of the SmartComponentBase class.

        :param update_interval: The interval at which the control loop should be executed, in seconds.
        """
        self.update_interval = update_interval
        self._status: RunnableStatus = RunnableStatus.NOT_STARTED
        self.terminated = asyncio.Event()
        self._loop_iterations = 0
        self._loop_avg_control_time = 0.0
        self._loop_max_control_time = 0.0
        self._loop_last_control_time = 0.0
        self._loop_avg_sleep_lag = 0.0
        self._loop_max_sleep_lag = 0.0
        self._loop_last_sleep_lag = 0.0
        self._loop_overrun_count = 0
        self._loop_started_at: float = 0.0

    @property
    def status(self):
        """
        Get the current status of the smart component.

        :return: The current status of the smart component.
        """
        return self._status

    def start(self):
        """
        Start the control loop of the smart component.
        If the component is not already started, it will start the control loop.
        """
        if self._status == RunnableStatus.NOT_STARTED:
            self.terminated.clear()
            self._status = RunnableStatus.RUNNING
            safe_ensure_future(self.control_loop())

    def stop(self):
        """
        Stop the control loop of the smart component.
        If the component is active or not started, it will stop the control loop.
        """
        if self._status != RunnableStatus.TERMINATED:
            self._status = RunnableStatus.TERMINATED
            self.terminated.set()

    async def control_loop(self):
        """
        The main control loop of the smart component.
        This method is responsible for executing the control task at the specified interval.
        """
        await self.on_start()
        self._loop_started_at = time.perf_counter()
        while not self.terminated.is_set():
            iteration_start = time.perf_counter()
            try:
                await self.control_task()
            except Exception as e:
                self.logger().error(e, exc_info=True)
            finally:
                control_duration = time.perf_counter() - iteration_start
                sleep_lag = 0.0
                if not self.terminated.is_set():
                    sleep_start = time.perf_counter()
                    await asyncio.sleep(self.update_interval)
                    sleep_lag = max(0.0, time.perf_counter() - sleep_start - self.update_interval)
                self._record_loop_metrics(control_duration=control_duration, sleep_lag=sleep_lag)
        self.on_stop()

    def on_stop(self):
        """
        Method to be executed when the control loop is stopped.
        This method should be overridden in subclasses to provide specific behavior.
        """
        pass

    async def on_start(self):
        """
        Method to be executed when the control loop is started.
        This method should be overridden in subclasses to provide specific behavior.
        """
        pass

    async def control_task(self):
        """
        The main task to be executed in the control loop.
        This method should be overridden in subclasses to provide specific behavior.
        """
        pass

    def _record_loop_metrics(self, control_duration: float, sleep_lag: float):
        self._loop_iterations += 1
        iterations = self._loop_iterations
        self._loop_last_control_time = control_duration
        self._loop_last_sleep_lag = sleep_lag
        self._loop_avg_control_time += (control_duration - self._loop_avg_control_time) / iterations
        self._loop_avg_sleep_lag += (sleep_lag - self._loop_avg_sleep_lag) / iterations
        self._loop_max_control_time = max(self._loop_max_control_time, control_duration)
        self._loop_max_sleep_lag = max(self._loop_max_sleep_lag, sleep_lag)
        if control_duration > self.update_interval:
            self._loop_overrun_count += 1

    def get_loop_metrics(self) -> Dict[str, float]:
        overrun_ratio = self._loop_overrun_count / self._loop_iterations if self._loop_iterations > 0 else 0.0
        uptime = time.perf_counter() - self._loop_started_at if self._loop_started_at > 0 else 0.0
        return {
            "update_interval": self.update_interval,
            "iterations": self._loop_iterations,
            "uptime_seconds": uptime,
            "avg_control_time": self._loop_avg_control_time,
            "max_control_time": self._loop_max_control_time,
            "last_control_time": self._loop_last_control_time,
            "avg_sleep_lag": self._loop_avg_sleep_lag,
            "max_sleep_lag": self._loop_max_sleep_lag,
            "last_sleep_lag": self._loop_last_sleep_lag,
            "overrun_count": self._loop_overrun_count,
            "overrun_ratio": overrun_ratio,
        }

    def format_loop_metrics(self) -> str:
        metrics = self.get_loop_metrics()
        if metrics["iterations"] == 0:
            return f"update_interval={self.update_interval:.3f}s (warming up)"
        overrun_pct = metrics["overrun_ratio"] * 100
        return (
            f"update={metrics['update_interval']:.3f}s | "
            f"control avg/max={metrics['avg_control_time'] * 1000:.1f}/{metrics['max_control_time'] * 1000:.1f}ms | "
            f"sleep_lag avg/max={metrics['avg_sleep_lag'] * 1000:.1f}/{metrics['max_sleep_lag'] * 1000:.1f}ms | "
            f"overrun={overrun_pct:.1f}% ({int(metrics['overrun_count'])}/{int(metrics['iterations'])})"
        )
