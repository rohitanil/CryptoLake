import requests
from datetime import datetime
from src.utilities.enums import Constants


def flatten_json(data, parent_key='', sep='_'):
    """Recursively flattens a nested dictionary."""
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def parse_weather_response(api_response: dict) -> dict:
    """Parses and flattens the weather API response."""
    flat_data = flatten_json(api_response)
    flat_data['timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    return flat_data


def get_weather_data(state, api_key):
    base_url = Constants.WEATHER_URL.value

    try:
        response = requests.get(
            base_url.format(state=state, key=api_key),
            headers={"Content-type": "application/json; charset=UTF-8"}
        ).json()

        output = parse_weather_response(response)
        return output
    except Exception as e:
        print(f"Failed to fetch weather for {state}: {e}")

