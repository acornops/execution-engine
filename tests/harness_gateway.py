import asyncio
import json

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from execution_engine.examples import EXAMPLE_TOOL_RUN_ID

app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "ok"}

@app.post("/api/v1/llm/generations:stream")
async def stream(request: Request):
    data = await request.json()
    run_id = data.get("run_id", "")
    messages = data.get("messages", [])

    async def event_generator():
        # Check if we've already handled the tool call in this conversation.
        # ReAct feeds tool outputs back inside one bounded evidence message.
        has_tool_response = any(
            m.get("role") == "tool"
            or (
                m.get("role") == "user"
                and isinstance(m.get("content"), str)
                and "Live tool results:" in m["content"]
            )
            for m in messages
        )

        if run_id == EXAMPLE_TOOL_RUN_ID and not has_tool_response:
            # Emit a tool call
            yield json.dumps({
                "type": "tool_call",
                "call_id": "a8b9c070-3cd3-415d-9f89-6f6f54e35d5d",
                "tool": "get_weather",
                "arguments": {"location": "San Francisco"}
            }) + "\n"
            await asyncio.sleep(0.1)
            return # Stop here, wait for next call with tool result

        if has_tool_response:
            # After tool result, emit final answer
            words = [" The", " weather", " in", " SF", " is", " sunny", "."]
        else:
            words = ["Hello", " this", " is", " a", " streamed", " response", "."]

        for word in words:
            yield json.dumps({"type": "delta", "text": word}) + "\n"
            await asyncio.sleep(0.1)

        # Yield final
        yield json.dumps(
            {
                "type": "final",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": len(words),
                    "tool_calls": 1 if run_id == EXAMPLE_TOOL_RUN_ID else 0,
                },
            }
        ) + "\n"

    return StreamingResponse(event_generator(), media_type="application/x-ndjson")

@app.post("/api/v1/mcp/tool-call")
async def tool_call(request: Request):
    data = await request.json()
    return {
        "result": f"Mock result for {data['tool']} with {data['arguments']}",
        "is_error": False
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
