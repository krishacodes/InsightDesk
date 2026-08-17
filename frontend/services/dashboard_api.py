import requests


API_BASE_URL = "http://127.0.0.1:8000"


def get_overview_data():

    response = requests.get(
        f"{API_BASE_URL}/dashboard/overview",
        timeout=10,
    )

    response.raise_for_status()

    return response.json()