import requests


API_BASE_URL = "http://127.0.0.1:8000"


def get_overview_data():

    response = requests.get(
        f"{API_BASE_URL}/dashboard/overview",
        timeout=30,
    )

    response.raise_for_status()

    return response.json()
def get_spikes():

    response = requests.get(
        f"{API_BASE_URL}/dashboard/spikes",
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def escalate_case(case_id):

    response = requests.post(
        f"{API_BASE_URL}/dashboard/cases/{case_id}/escalate",
        timeout=30,
    )

    response.raise_for_status()

    return response.json()