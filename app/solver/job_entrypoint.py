"""Standalone entrypoint executed by the Ray cluster for a solve job.

Ray Jobs execute a *command*, not a function pointer, so the CP-SAT solve is
wrapped here. The script:

1. parses ``--task-id`` and the base64-encoded routing payload,
2. initialises its own local Ray context (no Ray Client involved),
3. runs the CP-SAT solver,
4. writes the JSON itinerary to Redis at ``payanam:solve_result:{task_id}``
   with a 10-minute TTL, then exits cleanly.

The Temporal worker never imports ray; it submits this job over the Ray Job
REST API and polls for the Redis key. That split is what removes the Ray Client
thread-safety problem that made in-activity dispatch fail.
"""
from __future__ import annotations

import argparse
import base64
import json
import logging
import os
import sys

RESULT_TTL_SECONDS = 600  # 10 minutes


def _result_key(task_id: str) -> str:
    return f"payanam:solve_result:{task_id}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Payanam CP-SAT Ray job")
    parser.add_argument("--task-id", required=True)
    parser.add_argument(
        "--payload-b64",
        required=True,
        help="base64-encoded JSON RoutingRequest",
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis:6379/0"),
    )
    parser.add_argument("--time-limit", type=float, default=5.0)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    log = logging.getLogger("payanam.job")

    # Local (in-cluster) context. No Ray Client, therefore no thread-safety
    # constraints and no client/cluster version negotiation.
    import ray

    if not ray.is_initialized():
        ray.init(address="auto", log_to_driver=True, ignore_reinit_error=True)
    log.info("ray ready in job, nodes=%s", ray.nodes().__len__())

    payload = json.loads(base64.b64decode(args.payload_b64).decode("utf-8"))

    # Imported lazily so the module-level import graph stays light.
    from app.models import RoutingRequest
    from app.solver.cp_router import solve_local

    request = RoutingRequest.model_validate(payload)
    solution = solve_local(request, time_limit_seconds=args.time_limit)
    result = solution.model_dump()

    import redis as redis_lib

    client = redis_lib.from_url(args.redis_url, decode_responses=True)
    key = _result_key(args.task_id)
    client.set(key, json.dumps(result), ex=RESULT_TTL_SECONDS)
    log.info(
        "wrote %s (%d bytes, ttl=%ss) feasible=%s",
        key, len(json.dumps(result)), RESULT_TTL_SECONDS, result.get("feasible"),
    )
    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())