"""Pure Engine schema rules, shared by compatibility tests and the isolated worker."""

import re
from typing import Any

MAX_VALIDATION_ERRORS = 12


def validate_tool_arguments(value: Any, schema: Any) -> list[dict[str, str]]:
    """Validate the JSON-schema subset advertised by platform tools."""
    errors: list[dict[str, str]] = []

    def add(path: str, message: str) -> None:
        if len(errors) < MAX_VALIDATION_ERRORS:
            errors.append({"path": path, "message": message})

    def visit(item: Any, rule: Any, path: str) -> bool:
        if not isinstance(rule, dict):
            return True
        if "oneOf" in rule:
            candidates = [candidate for candidate in rule["oneOf"] if isinstance(candidate, dict)]
            evaluated = [(candidate, _candidate_errors(item, candidate, path)) for candidate in candidates]
            matches = [candidate for candidate, candidate_errors in evaluated if not candidate_errors]
            if len(matches) != 1:
                if not matches and evaluated:
                    best = min((candidate_errors for _, candidate_errors in evaluated), key=len)
                    for detail in best:
                        add(detail["path"], detail["message"])
                else:
                    add(path, "value must match exactly one allowed shape")
                return False
            return visit(item, matches[0], path)
        if "anyOf" in rule:
            candidates = [candidate for candidate in rule["anyOf"] if isinstance(candidate, dict)]
            evaluated = [(candidate, _candidate_errors(item, candidate, path)) for candidate in candidates]
            matches = [candidate for candidate, candidate_errors in evaluated if not candidate_errors]
            if not matches:
                if evaluated:
                    best = min((candidate_errors for _, candidate_errors in evaluated), key=len)
                    for detail in best:
                        add(detail["path"], detail["message"])
                else:
                    add(path, "value does not match an allowed shape")
                return False
            return visit(item, matches[0], path)
        expected_type = rule.get("type")
        if expected_type and not _type_matches(item, expected_type):
            add(path, f"expected {expected_type}")
            return False
        if "const" in rule and item != rule["const"]:
            add(path, f"expected constant {rule['const']!r}")
            return False
        if isinstance(rule.get("enum"), list) and item not in rule["enum"]:
            add(path, "value is not in the allowed enum")
            return False
        if isinstance(item, dict):
            required = rule.get("required") if isinstance(rule.get("required"), list) else []
            for key in required:
                if key not in item:
                    add(f"{path}.{key}", "required field is missing")
            properties = rule.get("properties") if isinstance(rule.get("properties"), dict) else {}
            if rule.get("additionalProperties") is False:
                for key in item:
                    if key not in properties:
                        add(f"{path}.{key}", "additional field is not allowed")
            for key, child in item.items():
                if key in properties:
                    visit(child, properties[key], f"{path}.{key}")
        elif isinstance(item, list):
            if isinstance(rule.get("minItems"), int) and len(item) < rule["minItems"]:
                add(path, f"requires at least {rule['minItems']} item(s)")
            if isinstance(rule.get("maxItems"), int) and len(item) > rule["maxItems"]:
                add(path, f"allows at most {rule['maxItems']} item(s)")
            for index, child in enumerate(item):
                visit(child, rule.get("items", {}), f"{path}[{index}]")
        elif isinstance(item, str):
            if isinstance(rule.get("minLength"), int) and len(item) < rule["minLength"]:
                add(path, f"requires at least {rule['minLength']} character(s)")
            if isinstance(rule.get("maxLength"), int) and len(item) > rule["maxLength"]:
                add(path, f"allows at most {rule['maxLength']} character(s)")
            if isinstance(rule.get("pattern"), str) and re.search(rule["pattern"], item) is None:
                add(path, "value does not match the required pattern")
        elif isinstance(item, (int, float)) and not isinstance(item, bool):
            if isinstance(rule.get("minimum"), (int, float)) and item < rule["minimum"]:
                add(path, f"must be at least {rule['minimum']}")
            if isinstance(rule.get("maximum"), (int, float)) and item > rule["maximum"]:
                add(path, f"must be at most {rule['maximum']}")
        return not errors

    def _candidate_errors(item: Any, rule: Any, path: str) -> list[dict[str, str]]:
        outer = list(errors)
        errors.clear()
        visit(item, rule, path)
        candidate_errors = list(errors)
        errors[:] = outer
        return candidate_errors

    visit(value, schema, "$")
    return errors


def _type_matches(value: Any, expected: Any) -> bool:
    allowed = expected if isinstance(expected, list) else [expected]
    return any({
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "boolean": isinstance(value, bool),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "null": value is None,
    }.get(kind, True) for kind in allowed)
