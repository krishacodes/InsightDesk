import requests


API_BASE_URL = "http://127.0.0.1:8000"


def get_cases():

    response = requests.get(
        f"{API_BASE_URL}/dashboard/cases",
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def get_case_intelligence(case_id):

    response = requests.get(
        f"{API_BASE_URL}/dashboard/cases/{case_id}/intelligence",
        timeout=30,
    )

    response.raise_for_status()

    return response.json()