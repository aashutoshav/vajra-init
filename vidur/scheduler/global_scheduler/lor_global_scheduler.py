from typing import List, Tuple

from vidur.entities import Request
from vidur.scheduler.global_scheduler.base_global_scheduler import BaseGlobalScheduler


class LORGlobalScheduler(BaseGlobalScheduler):
    """
    Least outstanding requests (LOR) global scheduler.
    """

    def mark_replica_to_free(self) -> int | None:
        """
        Part 1, Task 2
        Mark the replica with the least number of outstanding requests to be freed.
        Return the replica id marked to be freed.
        HINTS:
        1. The mark_replica_to_free method is required to mark the replica to free (i.e add the identified replica to the replicas_to_free set)
           and also return the marked replica id as hinted through the function return signature.
        """
        elig_reps = {
            rep_id: scheduler
            for rep_id, scheduler in self._replica_schedulers.items()
            if not self.check_replica_to_free(rep_id)
        }
        
        if not elig_reps:
            return None
        
        rep_loads = {
            rep_id: scheduler.num_pending_requests
            for rep_id, scheduler in elig_reps.items()
        }

        if not rep_loads:
            return None
        
        rep_ = min(rep_loads, key=rep_loads.get)
        self._replicas_to_free.add(rep_)
        return rep_
        
    def schedule(self) -> List[Tuple[int, Request]]:
        """
        Part 1, Task 2
        Route requests to replicas based on the number of outstanding requests.
        """
        self.sort_requests()
        req_mappings = []
        
        schedulable_replicas = sorted([
            rep_id for rep_id in self._replica_schedulers
            if not self.check_replica_to_free(rep_id)
        ])

        if not schedulable_replicas:
            return req_mappings

        rep_loads = {
            rep_id: self.get_replica_scheduler(rep_id).num_pending_requests
            for rep_id in schedulable_replicas
        }
        
        while self._request_queue:
            request = self._request_queue.pop(0)

            best_replica_id = min(rep_loads, key=rep_loads.get)

            req_mappings.append((best_replica_id, request))

            rep_loads[best_replica_id] += 1

        return req_mappings
