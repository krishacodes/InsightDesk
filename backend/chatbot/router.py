from fastapi import APIRouter
from pydantic import BaseModel

from langchain_core.messages import HumanMessage, AIMessage, ToolMessage

from backend.chatbot.graph import chatbot_graph


router = APIRouter(
    prefix="/chat",
    tags=["Chatbot"]
)


class ChatRequest(BaseModel):
    message: str


class ChatResponse(BaseModel):
    answer: str


@router.post("", response_model=ChatResponse)
def chat(request: ChatRequest):

    result = chatbot_graph.invoke(
        {
            "messages": [
                HumanMessage(
                    content=request.message
                )
            ]
        }
    )

    messages = result["messages"]

    # Find the final AI response
    for message in reversed(messages):

        if isinstance(message, AIMessage):

            # Ignore an AI message that only requested a tool
            if message.tool_calls:
                continue

            return ChatResponse(
                answer=message.content
            )

    return ChatResponse(
        answer="I was unable to generate a response."
    )