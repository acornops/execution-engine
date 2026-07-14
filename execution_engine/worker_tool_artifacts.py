"""Short-lived complete tool-result artifact persistence."""

import re
from typing import Any

from execution_engine.agent.tool_context import compact_tool_context, json_bytes
from execution_engine.orchestrator_client import OrchestratorClient
from execution_engine.util.logging import logger
from execution_engine.util.metrics import tool_context_bytes, tool_result_artifacts_total

ARTIFACT_METADATA_FIELDS = (
    "id", "expires_at", "sha256", "uncompressed_bytes", "compressed_bytes", "content_type",
)
UUID_V4_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", re.I)
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
RFC3339_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z$")


def _valid_artifact_metadata(value: Any) -> bool:
    """Return whether the control-plane upload receipt can safely enter a run event."""
    return (
        isinstance(value, dict)
        and all(key in value for key in ARTIFACT_METADATA_FIELDS)
        and all(isinstance(value[key], str) for key in ("id", "expires_at", "sha256", "content_type"))
        and all(isinstance(value[key], int) and value[key] >= 0 for key in ("uncompressed_bytes", "compressed_bytes"))
        and bool(UUID_V4_RE.fullmatch(value["id"]))
        and bool(RFC3339_RE.fullmatch(value["expires_at"]))
        and bool(SHA256_RE.fullmatch(value["sha256"]))
        and value["content_type"] in {"application/json", "text/plain"}
    )


def _nonnegative_int(value: Any, fallback: int) -> int:
    """Parse trusted size metadata without letting malformed local chunks break a run."""
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else fallback


async def persist_tool_result_artifact(
    orchestrator_client: OrchestratorClient,
    run_id: str,
    chunk: dict[str, Any],
) -> tuple[dict[str, Any] | None, bool]:
    """Persist an eligible complete result without failing the diagnosis."""
    context_size = int((chunk.get("context_meta") or {}).get("context_bytes") or 0)
    tool_context_bytes.observe(context_size)
    if not bool(chunk.get("artifact_eligible")):
        return None, False
    try:
        artifact = await orchestrator_client.create_tool_result_artifact(
            run_id,
            call_id=str(chunk["call_id"]),
            tool_name=str(chunk["tool"]),
            result=chunk.get("full_result"),
        )
        if not _valid_artifact_metadata(artifact):
            raise ValueError("control plane returned invalid artifact metadata")
        tool_result_artifacts_total.labels(result="success").inc()
        return artifact, False
    except Exception:
        tool_result_artifacts_total.labels(result="failure").inc()
        logger.warning(
            "tool_result_artifact_persist_failed",
            extra={"tool": str(chunk["tool"]), "call_id": str(chunk["call_id"])},
        )
        return None, True


def tool_result_event_payload(
    chunk: dict[str, Any], artifact: dict[str, Any] | None, artifact_unavailable: bool
) -> dict[str, Any]:
    """Build the compact-only durable tool completion event."""
    supplied_result = chunk.get("result")
    result = compact_tool_context(supplied_result)
    supplied_meta = chunk.get("context_meta")
    meta = supplied_meta if isinstance(supplied_meta, dict) else {}
    omissions = meta.get("omissions") if isinstance(meta.get("omissions"), list) else []
    context_meta = {
        "schema_version": "v1",
        "strategy": str(meta.get("strategy") or "local_structural_fallback")[:64],
        "original_bytes": _nonnegative_int(meta.get("original_bytes"), json_bytes(supplied_result)),
        "context_bytes": json_bytes(result),
        "truncated": bool(meta.get("truncated")) or result != supplied_result,
        "omissions": omissions,
    }
    artifact_meta = None
    if _valid_artifact_metadata(artifact):
        artifact_meta = {
            key: artifact[key]
            for key in ARTIFACT_METADATA_FIELDS
        }
    return {
        "call_id": chunk["call_id"],
        "tool": chunk["tool"],
        "result": result,
        "context_meta": context_meta,
        **({"artifact": artifact_meta} if artifact_meta else {}),
        **({"artifactUnavailable": True} if artifact_unavailable else {}),
        "is_error": bool(chunk["is_error"]),
    }
