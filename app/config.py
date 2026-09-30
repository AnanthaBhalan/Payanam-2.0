"""Environment configuration for Project payanam.

Every setting defaults to a ``localhost`` address so the stack boots cleanly on
a developer laptop; the docker-compose stack overrides them with in-network
hostnames via environment variables.
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import List

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    # ------------------------------------------------------------------ app
    app_env: str = Field(default="local")
    app_name: str = "payanam"
    log_level: str = Field(default="INFO")

    # -------------------------------------------------------------- memgraph
    memgraph_uri: str = Field(default="bolt://localhost:7687")
    memgraph_user: str = Field(default="")
    memgraph_password: str = Field(default="")
    memgraph_database: str = Field(default="memgraph")
    memgraph_max_pool: int = Field(default=50)

    # -------------------------------------------------------------- redpanda
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_topic_traffic: str = Field(default="traffic_updates")
    kafka_topic_bookings: str = Field(default="booking_events")
    kafka_topic_results: str = Field(default="itinerary_results")
    kafka_consumer_group: str = Field(default="payanam-edge-weights")
    kafka_auto_offset_reset: str = Field(default="latest")

    # -------------------------------------------------------------- temporal
    temporal_host: str = Field(default="localhost:7233")
    temporal_namespace: str = Field(default="payanam")
    temporal_task_queue: str = Field(default="payanam-routing")
    temporal_workflow_id_prefix: str = Field(default="payanam-")
    # Grace window (seconds) before the first booking, during which a live
    # transit_update signal can still amend the plan. 0 disables the window.
    replan_grace_seconds: float = Field(default=0.0, ge=0.0, le=60.0)
    # A leg whose live confirmation probability falls below this is abandoned.
    degraded_threshold: float = Field(default=0.15, ge=0.0, le=1.0)

    # ---------------------------------------------------------------- redis
    # Fleet geospatial index. PAYANAM_REDIS_URL="" falls back to an in-process
    # fakeredis instance so the stack (and tests) run without a Redis server.
    redis_url: str = Field(default="redis://localhost:6379/0")
    fleet_geo_key: str = Field(default="payanam:fleet:available")
    fleet_locked_key: str = Field(default="payanam:fleet:locked")
    fleet_driver_prefix: str = Field(default="payanam:fleet:driver:")
    fleet_lock_prefix: str = Field(default="payanam:fleet:lock:")
    # Seconds a dispatch hold survives before it is considered abandoned.
    fleet_lock_ttl_seconds: int = Field(default=120, ge=1)

    # ------------------------------------------------------------------- ray
    ray_address: str = Field(default="local")  # "local" in-process, else ray://host:6379
    ray_num_cpus: int = Field(default=2)

    # ---------------------------------------------------------------- solver
    solver_time_limit_seconds: float = Field(default=5.0)
    solver_num_workers: int = Field(default=8)

    @field_validator(
        "kafka_bootstrap_servers", "temporal_host", "memgraph_uri", "ray_address"
    )
    @classmethod
    def _strip(cls, v: str) -> str:
        return v.strip()

    @property
    def kafka_brokers(self) -> List[str]:
        return [b.strip() for b in self.kafka_bootstrap_servers.split(",") if b.strip()]

    @property
    def is_docker(self) -> bool:
        return self.app_env.lower() in {"docker", "compose", "prod", "production"}


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


settings = get_settings()
