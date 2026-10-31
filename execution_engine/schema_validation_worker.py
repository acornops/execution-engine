"""Standalone child: no service settings, credentials or application imports."""

import json
import runpy
import sys
from pathlib import Path


def validate(schema, instance):
    rules = runpy.run_path(str(Path(__file__).parent / "agent" / "tool_schema_rules.py"))
    return rules["validate_tool_arguments"](instance, schema)


def main():
    try:
        data = sys.stdin.buffer.read(3 * 1024 * 1024 + 1)
        if len(data) > 3 * 1024 * 1024:
            raise ValueError("request too large")
        request = json.loads(data)
        errors = validate(request["schema"], request["instance"])
        errors = [{key: detail[key].encode()[:256].decode(errors="ignore")
                   for key in ("path", "message")} for detail in errors[:12]]
        response = json.dumps({"errors": errors}, ensure_ascii=False).encode()
        while len(response) > 8192 and len(errors) > 1:
            errors.pop()
            response = json.dumps({"errors": errors}, ensure_ascii=False).encode()
    except Exception:
        response = b'{"failure":"validation"}'
    sys.stdout.buffer.write(response)


if __name__ == "__main__":
    main()
