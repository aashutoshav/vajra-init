import math
from collections import deque
from vidur.autoscaler.base_autoscaler import BaseAutoscaler
from vidur.config.config import CustomAutoscalerConfig
from vidur.entities.batch import Batch
from vidur.entities.cluster import Cluster
from vidur.entities.request import Request
from vidur.logger import init_logger
from vidur.metrics.metrics_store import MetricsStore
from vidur.scheduler.global_scheduler.base_global_scheduler import BaseGlobalScheduler

from vidur.autoscaler.inferline_autoscaler import NetworkEnvelope

logger = init_logger(__name__)

class CustomAutoscaler(BaseAutoscaler):
    def __init__(
        self,
        autoscaler_config: CustomAutoscalerConfig,
        cluster: Cluster,
        scheduler: BaseGlobalScheduler,
        metrics_store: MetricsStore,
    ) -> None:
        super().__init__(autoscaler_config, cluster, scheduler, metrics_store)
        """
        Initialize the autoscaler
        """
        self._replica_token_throughput = getattr(
            self._autoscaler_config, 
            'initial_replica_token_throughput', 
            1.0
        )
        self._throughput_alpha = getattr(
            self._autoscaler_config, 
            'throughput_alpha', 
            0.5
        )
        
        self._last_scale_up_time = float('-inf')

        self.init_service_level()
        
        class EnvelopeConfig:
            def __init__(self, min_window_up, look_back_up, min_window_down, look_back_down):
                self.min_window_size_scale_up = min_window_up
                self.look_back_time_scale_up = look_back_up
                self.min_window_size_scale_down = min_window_down
                self.look_back_time_scale_down = look_back_down
        
        envelope_config = EnvelopeConfig(
            self._min_window_up,
            self._look_back_up,
            self._min_window_down,
            self._look_back_down
        )
        self._network_envelope = NetworkEnvelope(envelope_config)

    def init_service_level(self) -> None:
        """
        Initialize the service level of the autoscaler
        Strategy: Aggressive spread based on "Stingy" baseline
        """
        level = self._autoscaler_config.service_level

        self._min_window_up = self._autoscaler_config.min_window_size_scale_up
        self._look_back_up = self._autoscaler_config.look_back_time_scale_up
        self._min_window_down = self._autoscaler_config.min_window_size_scale_down
        self._look_back_down = self._autoscaler_config.look_back_time_scale_down
        self._stabilization = self._autoscaler_config.stabilization_delay

        if level == 1:
            self.scale_up_utilization = 1.2
            self.scale_down_utilization = 1.1

        elif level == 2:
            self.scale_up_utilization = 1.0
            self.scale_down_utilization = 1.0

        elif level == 3:
            self.scale_up_utilization = 0.7
            self.scale_down_utilization = 0.6
            
    def on_request_arrival(self, request: Request) -> None:
        """
        Update required state when a request arrives.
        """
        self._network_envelope.on_request_arrival(request)

    def on_batch_end(self, batch: Batch) -> None:
        """
        Update required state when a batch ends.
        """
        total_tokens_processed = batch.total_num_tokens
        total_execution_time = batch.completed_at - batch.scheduled_at
        if total_execution_time > 0:
            current_throughput = total_tokens_processed / total_execution_time
            
            alpha = self._throughput_alpha
            self._replica_token_throughput = (alpha * current_throughput) + ((1 - alpha) * self._replica_token_throughput)

    def tune(self, time: float) -> int:
        """
        Args:
        - time: Current time
        Returns:
        - replicas: Number of replicas to scale up or down
            +ve value indicates scale up
            -ve value indicates scale down
            0 indicates no change
        """
        effective_replicas = (
            self.num_replicas 
            + self._num_pending_scale_ups 
            - self._num_pending_scale_downs
        )

        max_arrival_rate_up = self._network_envelope.get_max_request_rate(
            time, 
            self._min_window_up, 
            self._look_back_up   
        )
        
        target_replicas_up = 0
        if self._replica_token_throughput > 0:
            raw_target_replicas = max_arrival_rate_up / self._replica_token_throughput
            target_replicas_up = math.ceil(raw_target_replicas / self.scale_up_utilization)
        
        target_replicas_up = max(target_replicas_up, self._autoscaler_config.min_replicas)

        scale_up_delta = target_replicas_up - effective_replicas
        
        if scale_up_delta > 0:
            self._last_scale_up_time = time
            self._num_pending_scale_ups += scale_up_delta
            return scale_up_delta 

        if (time - self._last_scale_up_time) < self._stabilization: 
            return 0 

        max_arrival_rate_down = self._network_envelope.get_max_request_rate(
            time,
            self._min_window_down, 
            self._look_back_down   
        )
        
        target_replicas_down = 0
        if self._replica_token_throughput > 0:
            raw_target_replicas = max_arrival_rate_down / self._replica_token_throughput
            target_replicas_down = math.ceil(raw_target_replicas / self.scale_down_utilization)
            
        target_replicas_down = max(target_replicas_down, self._autoscaler_config.min_replicas)

        scale_down_delta = effective_replicas - target_replicas_down
        
        if scale_down_delta > 0:
            self._num_pending_scale_downs += scale_down_delta
            return -scale_down_delta

        return 0