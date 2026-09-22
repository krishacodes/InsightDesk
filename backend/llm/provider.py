import os
import json
import requests
from langchain_groq import ChatGroq
from langchain_ollama import ChatOllama
from groq import Groq
from dotenv import load_dotenv

load_dotenv()


LOCAL_PROVIDER = "local"
GROQ_PROVIDER = "groq"

DEFAULT_LOCAL_MODEL = "qwen3:1.7b"
DEFAULT_GROQ_MODEL = "openai/gpt-oss-120b"


def get_provider():
    return os.getenv(
        "LLM_PROVIDER",
        LOCAL_PROVIDER
    ).lower()


def get_model_name():
    provider = get_provider()

    if provider == LOCAL_PROVIDER:
        return os.getenv(
            "LOCAL_LLM_MODEL",
            DEFAULT_LOCAL_MODEL
        )

    if provider == GROQ_PROVIDER:
        return os.getenv(
            "GROQ_MODEL",
            DEFAULT_GROQ_MODEL
        )

    raise ValueError(
        f"Unsupported LLM_PROVIDER: {provider}"
    )


def _generate_local(prompt):
    model = get_model_name()

    base_url = os.getenv(
        "LOCAL_LLM_BASE_URL",
        "http://localhost:11434"
    ).rstrip("/")

    response = requests.post(
        f"{base_url}/api/chat",
        json={
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "stream": False,

            # Ask Ollama for structured JSON.
            "format": "json",

            "options": {
                "temperature": 0
            }
        },
        timeout=180
    )

    response.raise_for_status()

    data = response.json()

    return data["message"]["content"]


def _generate_groq(prompt):
    api_key = os.getenv("GROQ_API_KEY")

    if not api_key:
        raise RuntimeError(
            "GROQ_API_KEY is missing."
        )

    client = Groq(
        api_key=api_key
    )

    model = get_model_name()

    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0
    )

    return (
        response
        .choices[0]
        .message
        .content
    )


def generate_text(prompt):
    provider = get_provider()

    if provider == LOCAL_PROVIDER:
        return _generate_local(prompt)

    if provider == GROQ_PROVIDER:
        return _generate_groq(prompt)

    raise ValueError(
        f"Unsupported LLM_PROVIDER: {provider}"
    )


def generate_json(prompt):
    content = generate_text(prompt)

    content = (
        content
        .replace("```json", "")
        .replace("```JSON", "")
        .replace("```", "")
        .strip()
    )

    try:
        return json.loads(content)

    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "LLM returned invalid JSON.\n"
            f"Provider: {get_provider()}\n"
            f"Model: {get_model_name()}\n"
            f"Response:\n{content}"
        ) from exc
def get_chat_model():
    provider = get_provider()
    model = get_model_name()

    if provider == LOCAL_PROVIDER:
        return ChatOllama(
            model=model,
            base_url=os.getenv(
                "LOCAL_LLM_BASE_URL",
                "http://localhost:11434"
            ),
            temperature=0,
        )

    if provider == GROQ_PROVIDER:
        api_key = os.getenv("GROQ_API_KEY")

        if not api_key:
            raise RuntimeError(
                "GROQ_API_KEY is missing."
            )

        return ChatGroq(
            model=model,
            api_key=api_key,
            temperature=0,
        )

    raise ValueError(
        f"Unsupported LLM_PROVIDER: {provider}"
    )