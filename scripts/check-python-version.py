import sys

REQUIRED_MAJOR = 3
REQUIRED_MINOR = 12
REQUIRED_PATCH = 11


def main() -> None:
    """Validate that the current interpreter matches production Python."""
    if sys.version_info[:3] != (REQUIRED_MAJOR, REQUIRED_MINOR, REQUIRED_PATCH):
        current = ".".join(str(part) for part in sys.version_info[:3])
        required = f"{REQUIRED_MAJOR}.{REQUIRED_MINOR}.{REQUIRED_PATCH}"
        raise SystemExit(
            f"execution-engine requires Python {required}; current interpreter is Python {current}. "
            f"Recreate .venv with Python {required}."
        )


if __name__ == "__main__":
    main()
