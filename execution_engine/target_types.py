"""Shared target type declarations for execution-engine contracts."""

from typing import Literal

TargetType = Literal["kubernetes", "virtual_machine"]

KUBERNETES_TARGET_TYPE: TargetType = "kubernetes"
VIRTUAL_MACHINE_TARGET_TYPE: TargetType = "virtual_machine"
TARGET_TYPE_EXAMPLES = [KUBERNETES_TARGET_TYPE, VIRTUAL_MACHINE_TARGET_TYPE]
