import requests
from datetime import datetime
from src.producer.enums import Constants


def get_coin_price(coins, currency, api_key):
    """
    Method to get the crypto coin prices in the requested currency denominations
    :param coins: Comma seperated crypto coins
    :param currency: Comma seperated currencies
    :param api_key: API key for authorization
    :return: response: JSON response from the API
    """
    base_url = Constants.CRYPTO_URL.value
    request_url = base_url+"fsyms={c}&tsyms={curr}&api_key={api}". \
        format(c=coins, curr=currency, api=api_key)
    response = requests.get(request_url).json()
    response["timestamp"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    return response
