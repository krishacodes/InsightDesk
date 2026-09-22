import requests


API_BASE_URL = "http://127.0.0.1:8000"


def get_clusters_data():

    response = requests.get(
        f"{API_BASE_URL}/dashboard/clusters",
        timeout=30,
    )

    response.raise_for_status()

    return response.json()