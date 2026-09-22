from langchain_core.messages import SystemMessage

from langgraph.graph import (
    StateGraph,
    START,
)

from langgraph.prebuilt import (
    ToolNode,
    tools_condition,
)

from backend.chatbot.state import ChatbotState
from backend.chatbot.prompts import SYSTEM_PROMPT

from backend.chatbot.tools import (
    get_case_details,
    get_complaints_for_case,
    get_topic,
    list_topics,
    get_case_rca,
)

from backend.llm.provider import (
    get_chat_model,
    get_provider,
    get_model_name,
)


TOOLS = [
    get_case_details,
    get_complaints_for_case,
    get_topic,
    list_topics,
    get_case_rca,
]


model = get_chat_model()

model_with_tools = model.bind_tools(
    TOOLS
)


def agent_node(
    state: ChatbotState
):
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


if __name__ == "__main__":
    print(
        "Chatbot provider:",
        get_provider()
    )

    print(
        "Chatbot model:",
        get_model_name()
    )

    print(
        "Chatbot graph built successfully."
    )