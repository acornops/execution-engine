import json

from execution_engine.worker_fallbacks import build_tool_only_fallback


def test_tool_only_fallback_summarizes_list_pods_payload():
    tool_events = [
        {
            "tool": "list_pods",
            "is_error": False,
            "result": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "total": 2,
                            "namespace": "acornops-demo",
                            "pods": [
                                {
                                    "name": "healthy-pod",
                                    "namespace": "acornops-demo",
                                    "phase": "Running",
                                    "restartCount": 0,
                                },
                                {
                                    "name": "crashy-pod",
                                    "namespace": "acornops-demo",
                                    "phase": "Running",
                                    "restartCount": 4,
                                },
                            ],
                        }
                    ),
                }
            ],
        }
    ]

    summary = build_tool_only_fallback(tool_events)

    assert "potentially unhealthy pod(s)" in summary
    assert "`crashy-pod`" in summary
    assert "Healthy pods: 1." in summary


def test_tool_only_fallback_uses_generic_summary_for_unknown_tools():
    tool_events = [
        {
            "tool": "get_pod_logs",
            "is_error": False,
            "result": [{"type": "text", "text": "line one\nline two"}],
        }
    ]

    summary = build_tool_only_fallback(tool_events)

    assert "latest tool outputs" in summary.lower()
    assert "`get_pod_logs` (success)" in summary


def test_tool_only_fallback_builds_pod_diagnosis_from_describe_and_logs():
    tool_events = [
        {
            "tool": "describe_resource",
            "is_error": False,
            "result": [
                {
                    "type": "text",
                    "text": json.dumps(
                        {
                            "kind": "Pod",
                            "metadata": {
                                "name": "acornops-demo-unhealthy-6475944c54-gk6kg",
                                "namespace": "acornops-demo",
                            },
                            "status": {
                                "phase": "Running",
                                "containerStatuses": [
                                    {
                                        "name": "crash",
                                        "restartCount": 5,
                                        "state": {"waiting": {"reason": "CrashLoopBackOff"}},
                                    }
                                ],
                            },
                        }
                    ),
                }
            ],
        },
        {
            "tool": "get_pod_logs",
            "is_error": False,
            "result": [
                {
                    "type": "text",
                    "text": json.dumps({"logs": "Intentional failure for demo troubleshooting\n"}),
                }
            ],
        },
    ]

    summary = build_tool_only_fallback(tool_events)

    assert "I inspected pod" in summary
    assert "CrashLoopBackOff" in summary
    assert "intentional demo failure" in summary.lower()
