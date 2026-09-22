import requests


API_URL = "http://127.0.0.1:8000"


def send_chat_message(message: str) -> str:
    """
    Send a message to the InsightDesk chatbot API
    and return the assistant's answer.
    """

    response = requests.post(
        f"{API_URL}/chat",
        json={
            "message": message
        },
        timeout=180,
    )

    response.raise_for_status()

    data = response.json()

    return data.get(
        "answer",
        "No answer was returned."
    )