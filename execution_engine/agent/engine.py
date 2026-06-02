"""Abstract base class for agent reasoning engines."""

from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator, Dict, List

from execution_engine.models import Message


class AgentEngine(ABC):
    """
    Interface for agent execution.

    Subclasses should implement the reasoning loop (e.g., ReAct).
    """
    @abstractmethod
    async def run(
        self,
        messages: List[Message],
        llm_config: Any,
        cancel_event: Any
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Executes the agent logic.

        Args:
            messages: The initial conversation messages.

        Yields:
            Chunks of the assistant's response.
        """
        pass
