"""Async Kafka (Redpanda) consumer for live edge-probability updates.

Real-time traffic events land on the ``traffic_updates`` topic.  Each message
carries a multiplier for a ``TRANSIT`` edge which is:

1. recorded in an in-process registry read by the CP-SAT model builder, and
2. written back to Memgraph so the graph stays the source of truth.

The consumer is a plain asyncio task started from the FastAPI lifespan, so it
starts and stops deterministically with the app.
"""
from __future__ import annotations

import asyncio
import json
import logging
import signal
from dataclasses import dataclass, field
from typing import Dict, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException

from ..config import Settings, get_settings

log = logging.getLogger("payanam.ingestion")

VALID_MODES = {"SET", "MULTIPLY", "INCREMENT"}


@dataclass
class EdgeWeightRegistry:
    """Live view of edge multipliers, shared with the API layer."""

    multipliers: Dict[str, float] = field(default_factory=dict)
    events_seen: int = 0
    last_event_at: Optional[str] = None

    def apply(self, edge_id: str, multiplier: float, mode: str = "SET") -> float:
        mode = (mode or "SET").upper()
        current = self.multipliers.get(edge_id, 1.0)
        if mode == "MULTIPLY":
            value = current * multiplier
        elif mode == "INCREMENT":
            value = current + multiplier
        else:
            value = multiplier
        value = min(max(value, 0.1), 10.0)
        self.multipliers[edge_id] = value
        self.events_seen += 1
        return value

    def snapshot(self) -> Dict[str, float]:
        return dict(self.multipliers)


# Process-wide singleton; the /health endpoint reports on it.
REGISTRY = EdgeWeightRegistry()


# --------------------------------------------------------------------------- #
# Temporal signal forwarding (Phase 2)
# --------------------------------------------------------------------------- #
# Injectable client. The test harness sets this to a Temporal test client so no
# external server is required; production leaves it None and the sender below
# falls back to the process-wide client from app.temporal_client.
_TEMPORAL_CLIENT: Optional[object] = None


def set_temporal_client(client: Optional[object]) -> None:
    """Install the client used to signal running workflows."""
    global _TEMPORAL_CLIENT
    _TEMPORAL_CLIENT = client
    log.info("temporal signal client %s", "set" if client else "cleared")


async def _temporal_signal_sender(
    workflow_id: str,
    leg_id: str,
    p_confirm: float,
    delay_minutes: int = 0,
) -> None:
    """Send ``transit_update`` to a running ``ItinerarySaga``."""
    client = _TEMPORAL_CLIENT
    if client is None:
        from ..temporal_client import get_temporal_client

        client = get_temporal_client()

    handle = client.get_workflow_handle(workflow_id)
    # `args` (plural) unpacks into the handler's parameters; `arg` (singular)
    # would deliver the whole list as a single positional value.
    await handle.signal(
        "transit_update",
        args=[leg_id, float(p_confirm), int(delay_minutes)],
    )


class TrafficUpdateConsumer:
    """Bridges ``confluent_kafka``'s blocking API onto an asyncio loop."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        registry: Optional[EdgeWeightRegistry] = None,
        on_update=None,
        on_signal=None,
    ) -> None:
        self.settings = settings or get_settings()
        self.registry = registry or REGISTRY
        self._on_update = on_update
        # Override for the signal sender; None uses the Temporal client.
        self._on_signal = on_signal
        self._consumer: Optional[Consumer] = None
        self._task: Optional[asyncio.Task] = None
        self._stopping = asyncio.Event()

    # ------------------------------------------------------------- lifecycle
    def _build_consumer(self) -> Consumer:
        conf = {
            "bootstrap.servers": self.settings.kafka_brokers,
            "group.id": self.settings.kafka_consumer_group,
            "auto.offset.reset": self.settings.kafka_auto_offset_reset,
            "enable.auto.commit": False,  # at-least-once, explicit commits
        # Docker DNS: librdkafka's built-in resolver intermittently fails on
        # the compose service name and reports "Failed to resolve ... Missing
        # close-", even though getaddrinfo resolves it fine. Forcing IPv4 and
        # plaintext makes the lookup deterministic inside the network.
        "broker.address.family": "v4",
        "security.protocol": "PLAINTEXT",
        "socket.timeout.ms": 5000,
            "enable.partition.eof": True,
            "session.timeout.ms": 10000,
        }
        consumer = Consumer(conf)
        consumer.subscribe([self.settings.kafka_topic_traffic])
        return consumer

    def start(self) -> Optional[asyncio.Task]:
        if self._task and not self._task.done():
            return self._task
        try:
            self._consumer = self._build_consumer()
        except KafkaException as exc:
            # Kafka must never take the API down -- degrade to in-process only.
            log.error("kafka unavailable, live weights disabled: %s", exc)
            return None
        self._stopping.clear()
        self._task = asyncio.create_task(self._run(), name="traffic-update-consumer")
        log.info(
            "kafka consumer started on topic '%s' (brokers=%s)",
            self.settings.kafka_topic_traffic,
            self.settings.kafka_brokers,
        )
        return self._task

    async def stop(self) -> None:
        self._stopping.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # noqa: BLE001
                log.debug("consumer task ended with: %s", exc)
            self._task = None
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception as exc:  # noqa: BLE001
                log.debug("consumer close failed: %s", exc)
            self._consumer = None
        log.info("kafka consumer stopped")

    # ------------------------------------------------------------------- run
    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        assert self._consumer is not None
        while not self._stopping.is_set():
            try:
                # poll() blocks, so push it onto a worker thread
                msg = await loop.run_in_executor(None, self._consumer.poll, 1.0)
            except KafkaException as exc:
                log.error("kafka poll error: %s", exc)
                await asyncio.sleep(2.0)
                continue

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("kafka message error: %s", msg.error())
                continue

            await self._handle(msg)

    async def _handle(self, msg) -> None:
        raw = msg.value()
        try:
            event = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            log.warning("skipping malformed traffic event: %s", exc)
            return
        if not isinstance(event, dict):
            log.warning("skipping non-object traffic event: %r", event)
            return

        edge_id = event.get("edge_id") or event.get("edgeId") or event.get("leg_id")
        mode = str(event.get("mode", "SET")).upper()

        # -- phase 2: a disruption naming a running workflow is a Temporal
        # signal, not merely an edge-weight tweak.
        workflow_id = event.get("workflow_id")
        if workflow_id:
            await self._forward_signal(event, str(workflow_id))

        if mode not in VALID_MODES:
            log.warning("skipping invalid traffic event: %s", event)
            return
        if not edge_id:
            log.warning("skipping traffic event with no edge/leg id: %s", event)
            return
        raw_mult = event.get("multiplier", event.get("factor", 1.0))
        try:
            multiplier = float(raw_mult)
        except (TypeError, ValueError):
            log.warning("non-numeric multiplier in event: %s", event)
            return

        value = self.registry.apply(edge_id, multiplier, mode)
        log.info(
            "traffic update edge=%s mode=%s -> %.3f (partition=%s offset=%s)",
            edge_id, mode, value, msg.partition(), msg.offset(),
        )

        if self._on_update is not None:
            try:
                await self._on_update(edge_id, value)
            except Exception as exc:  # noqa: BLE001 - a sink must not kill the loop
                log.warning("edge update sink failed for %s: %s", edge_id, exc)

    # --------------------------------------------------- temporal forwarding
    async def dispatch(self, event: Dict[str, object]) -> None:
        """Process one already-decoded traffic event.

        Public so tests (and any non-Kafka producer) can drive the exact same
        fan-out path a broker message would take.
        """
        workflow_id = event.get("workflow_id")
        if workflow_id:
            await self._forward_signal(event, str(workflow_id))

        edge_id = event.get("edge_id") or event.get("leg_id")
        if not edge_id:
            return
        mode = str(event.get("mode", "SET")).upper()
        if mode not in VALID_MODES:
            return
        try:
            value = self.registry.apply(
                str(edge_id), float(event.get("multiplier", 1.0)), mode
            )
        except (TypeError, ValueError):
            log.warning("non-numeric multiplier in event: %s", event)
            return
        log.info("traffic update edge=%s -> %.3f", edge_id, value)
        if self._on_update is not None:
            await self._on_update(str(edge_id), value)

    async def _forward_signal(self, event: Dict[str, object], workflow_id: str) -> None:
        """Relay a ``traffic_updates`` message to the named workflow.

        Payload shape (Phase 2 Mandate)::

            {"workflow_id": "...", "leg_id": "...", "p_confirm": 0.05,
             "delay_minutes": 30}

        ``p_confirm`` is normalised to the signal's ``new_p_confirm``. A failure
        here is logged and swallowed: telemetry must never stop the consumer.
        """
        leg_id = str(event.get("leg_id") or event.get("edge_id") or "")
        if not leg_id:
            log.warning("workflow update with no leg_id; not forwarding: %s", event)
            return

        raw_p = event.get("p_confirm", event.get("new_p_confirm", 1.0))
        try:
            p_confirm = float(raw_p)
        except (TypeError, ValueError):
            log.warning("non-numeric p_confirm; not forwarding: %s", event)
            return
        try:
            delay = int(event.get("delay_minutes") or 0)
        except (TypeError, ValueError):
            delay = 0

        forwarder = self._signal_forwarder()
        if forwarder is None:
            log.warning(
                "no temporal client available; dropping signal for %s", workflow_id
            )
            return
        try:
            await forwarder(workflow_id, leg_id, p_confirm, delay)
            log.info(
                "forwarded transit_update workflow=%s leg=%s p=%.3f delay=%s",
                workflow_id, leg_id, p_confirm, delay,
            )
        except Exception as exc:  # noqa: BLE001 - never kill the consumer
            log.warning("signal forward failed for %s: %s", workflow_id, exc)

    def _signal_forwarder(self):
        """Resolve the signal sender, honouring an injected override."""
        if self._on_signal is not None:
            return self._on_signal
        return _temporal_signal_sender



async def main() -> None:  # pragma: no cover - container entrypoint
    """``python -m app.ingestion.stream`` -- run the consumer standalone."""
    logging.basicConfig(level=get_settings().log_level)
    from ..graph.state import AsyncMemgraphClient

    memgraph = AsyncMemgraphClient()

    async def sink(edge_id: str, multiplier: float) -> None:
        await memgraph.apply_edge_multiplier(edge_id, multiplier)

    consumer = TrafficUpdateConsumer(on_update=sink)
    await memgraph.connect()
    consumer.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, AttributeError):  # e.g. Windows
            pass
    try:
        await stop.wait()
    finally:
        await consumer.stop()
        await memgraph.close()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())

