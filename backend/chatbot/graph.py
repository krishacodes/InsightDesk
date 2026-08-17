import os

from dotenv import load_dotenv

from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage

from langgraph.graph import (
    StateGraph,
    START,
)

from langgraph.prebuilt import ToolNode, tools_condition

from backend.chatbot.state import ChatbotState
from backend.chatbot.prompts import SYSTEM_PROMPT

from backend.chatbot.tools import (
    get_case_details,
    get_complaints_for_case,
    get_topic,
    list_topics,
    get_case_rca,
)


load_dotenv()


TOOLS = [
    get_case_details,
    get_complaints_for_case,
    get_topic,
    list_topics,
    get_case_rca,
]


def get_chat_model():

    return ChatGroq(
        model="llama-3.3-70b-versatile",
        api_key=os.getenv("GROQ_API_KEY"),
        temperature=0,
    )


model = get_chat_model()

model_with_tools = model.bind_tools(TOOLS)


def agent_node(state: ChatbotState):

    messages = state["messages"]

    response = model_with_tools.invoke(
        [
            SystemMessage(
                content=SYSTEM_PROMPT
            ),
            *messages,
        ]
    )

    return {
        "messages": [response]
    }


def build_graph():

    graph = StateGraph(
        ChatbotState
    )

    graph.add_node(
        "agent",
        agent_node
    )

    graph.add_node(
        "tools",
        ToolNode(TOOLS)
    )

    graph.add_edge(
        START,
        "agent"
    )

    graph.add_conditional_edges(
        "agent",
        tools_condition
    )

    graph.add_edge(
        "tools",
        "agent"
    )

    return graph.compile()


chatbot_graph = build_graph()