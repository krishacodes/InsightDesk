from langchain_core.messages import HumanMessage

from backend.chatbot.graph import chatbot_graph


result = chatbot_graph.invoke(
    {
        "messages": [
            HumanMessage(
                content="What complaints are associated with case 71?"
            )
        ]
    }
)


for message in result["messages"]:
    print("\n---")
    print(message)