import requests


def make_circleci_items_request(url: str, circleci_token: str, params: dict) -> requests.Response:
    if not circleci_token:
        raise ValueError("CIRCLECI_TOKEN environment variable is not set")

    headers = {
        "Circle-Token": circleci_token
    }

    pagination_token = None

    items = []

    if not params:
        params = {}

    while True:
        if pagination_token:
            params["page-token"] = pagination_token

        response = requests.get(
            url,
            headers=headers,
            params=params
        )
        if response.status_code == 200:
            data = response.json()
            page_items = data.get("items", [])
            items.extend(page_items)
            pagination_token = data.get("next_page_token")
            if not pagination_token:
                break
        else:
            break

    return items


def make_circleci_item_request(url: str, circleci_token: str, params: dict) -> requests.Response:
    if not circleci_token:
        raise ValueError("CIRCLECI_TOKEN environment variable is not set")

    headers = {
        "Circle-Token": circleci_token
    }

    response = requests.get(
        url,
        headers=headers,
        params=params
    )

    data = None

    if response.status_code == 200:
        data = response.json()

    return data
      
