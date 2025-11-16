import math
from collections import deque
from vidur.autoscaler.base_autoscaler import BaseAutoscaler
from vidur.config.config import InferlineAutoscalerConfig
from vidur.entities.batch import Batch
from vidur.entities.cluster import Cluster
from vidur.entities.request import Request
from vidur.logger import init_logger
from vidur.metrics.metrics_store import MetricsStore
from vidur.scheduler.global_scheduler.base_global_scheduler import BaseGlobalScheduler

logger = init_logger(__name__)

"""
Network Traffic Envelope maintains a sliding window of request arrival token rates.
"""
class NetworkEnvelope:
    def __init__(self, autoscaler_config: InferlineAutoscalerConfig) -> None:
        """
        You will need the following fields from the Inferline Autoscaler config:
        - min_window_size_scale_up: Minimum window size for scale up
        - min_window_size_scale_down: Minimum window size for scale down
        - look_back_time_scale_up: Look back time for scale up
        - look_back_time_scale_down: Look back time for scale down
        """
        self.config = autoscaler_config
        self.arrivals = deque()

    def on_request_arrival(self, request: Request) -> None:
        """
        Update the network envelope with the arrival of a new request.
        """
        tokens = request.num_prefill_tokens + request.num_decode_tokens
        self.arrivals.append((request.arrived_at, tokens))

    def get_max_request_rate(self, time: float, window_size: float, look_back_time: float) -> float:
        """
        Args:
        - time: Current time
        - window_size: Window size for calculating the request token rates
        - look_back_time: Look back time for calculating the max request token rate
        
        Returns:
        - max_request_rate: Maximum request token rate in the window of size window_size in the duration between time - look_back_time and time
        """
        if window_size <= 0:
            return 0.0
        
        start_time = time - look_back_time
        while self.arrivals and self.arrivals[0][0] < start_time:
            self.arrivals.popleft()
            
        if not self.arrivals:
            return 0.0
        
        arrivals_list = list(self.arrivals)
        n = len(arrivals_list)
        
        if n == 0:
            return 0.0
        
        max_rate = 0.0
        
        right = 0
        current_tokens_start = 0.0
        for left in range(n):
            window_start = arrivals_list[left][0]
            window_end = window_start + window_size

            while right < n and arrivals_list[right][0] < window_end:
                current_tokens_start += arrivals_list[right][1]
                right += 1
            
            if window_start >= start_time and window_end <= time:
                current_rate = current_tokens_start / window_size
                max_rate = max(max_rate, current_rate)
            
            current_tokens_start -= arrivals_list[left][1]

        left = 0
        current_tokens_end = 0.0
        for right in range(n):
            window_end = arrivals_list[right][0]
            window_start = window_end - window_size
            
            current_tokens_end += arrivals_list[right][1]
            
            while left < right and arrivals_list[left][0] <= window_start:
                current_tokens_end -= arrivals_list[left][1]
                left += 1

            if window_start >= start_time and window_end <= time:
                current_rate = current_tokens_end / window_size
                max_rate = max(max_rate, current_rate)
        
        return max_rate

class InferlineAutoscaler(BaseAutoscaler):
    def __init__(
        self,
        autoscaler_config: InferlineAutoscalerConfig,
        cluster: Cluster,
        scheduler: BaseGlobalScheduler,
        metrics_store: MetricsStore,
    ) -> None:
        super().__init__(autoscaler_config, cluster, scheduler, metrics_store)

        """
        You will need the following fields from the Inferline Autoscaler config for maintaining and updating the replica token throughput:
        - initial_replica_token_throughput: Initial replica token throughput
        - throughput_alpha: Alpha value for exponential moving average of replica token throughput
        """    
    
        self._replica_token_throughput = self._autoscaler_config.initial_replica_token_throughput
        self._throughput_alpha = self._autoscaler_config.throughput_alpha
        
        self._network_envelope = NetworkEnvelope(autoscaler_config)
        self._last_scale_up_time = float('-inf')

    @property
    def replica_token_throughput(self) -> float:
        return self._replica_token_throughput

    def on_request_arrival(self, request: Request) -> None:
        """
        Update required state when a request arrives.
        """
        self._network_envelope.on_request_arrival(request)

    def on_batch_end(self, batch: Batch) -> None:
        """
        Update the replica token throughput based on completed batch. Use exponential moving average to update the replica token throughput.
        """
        total_tokens_processed = batch.total_num_tokens
        total_execution_time = batch.completed_at - batch.scheduled_at

        if total_execution_time > 0:
            current_throughput = total_tokens_processed / total_execution_time
            
            alpha = self._throughput_alpha
            self._replica_token_throughput = (alpha * current_throughput) + ((1 - alpha) * self._replica_token_throughput)
    
    def tune(self, time: float) -> int:
        """
        Inferline based autoscaler tuning.
        Args:
        - time: Current time
        Returns:
        - replicas: Number of replicas to scale up or down
            +ve value indicates scale up
            -ve value indicates scale down
            0 indicates no change

        HINTS:
        1. The autoscaler config field stabilization_delay is the time to wait before scaling down after the last scale up.
        2. Check for scale up first, then scale down.
        3. self._num_pending_scale_ups and self._num_pending_scale_downs are the number of pending scale ups and scale downs respectively. Make sure to account for these.
        """
        effective_replicas = (
            self.num_replicas 
            + self._num_pending_scale_ups 
            - self._num_pending_scale_downs
        )

        max_arrival_rate_up = self._network_envelope.get_max_request_rate(
            time, 
            self._autoscaler_config.min_window_size_scale_up, 
            self._autoscaler_config.look_back_time_scale_up
        )
        
        if self._replica_token_throughput > 0:
            target_replicas_up = math.ceil(max_arrival_rate_up / self._replica_token_throughput)
        else:
            target_replicas_up = 0
        
        scale_up_delta = target_replicas_up - effective_replicas
        
        if scale_up_delta > 0:
            self._last_scale_up_time = time
            self._num_pending_scale_ups += scale_up_delta
            return scale_up_delta 

        if (time - self._last_scale_up_time) < self._autoscaler_config.stabilization_delay:
            return 0 

        max_arrival_rate_down = self._network_envelope.get_max_request_rate(
            time,
            self._autoscaler_config.min_window_size_scale_down,
            self._autoscaler_config.look_back_time_scale_down
        )
        
        if self._replica_token_throughput > 0:
            target_replicas_down = math.ceil(max_arrival_rate_down / self._replica_token_throughput)
        else:
            target_replicas_down = 0
            
        scale_down_delta = effective_replicas - target_replicas_down
        
        if scale_down_delta > 0:
            if (effective_replicas - scale_down_delta) < self._autoscaler_config.min_replicas:
                scale_down_delta = effective_replicas - self._autoscaler_config.min_replicas
                if scale_down_delta <= 0:
                    return 0

            self._num_pending_scale_downs += scale_down_delta
            return -scale_down_delta

        return 0