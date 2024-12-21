import requests
from datetime import datetime
from src.utilities.enums import KafkaConstants


def get_coin_price(coins, currency, api_key):
    """
    Method to get the crypto coin prices in the requested currency denominations
    :param coins: Comma seperated crypto coins
    :param currency: Comma seperated currencies
    :param api_key: API key for authorization
    :return: response: JSON response from the API
    """
    base_url = KafkaConstants.CRYPTO_URL.value
    request_url = base_url+"fsyms={c}&tsyms={curr}&api_key={api}". \
        format(c=coins, curr=currency, api=api_key)
    response = requests.get(request_url).json()
    output = dict()
    output['BTC_EUR'] = response.get('BTC').get('EUR')
    output['BTC_INR'] = response.get('BTC').get('INR')
    output['BTC_USD'] = response.get('BTC').get('USD')
    output['ETH_EUR'] = response.get('BTC').get('EUR')
    output['ETH_INR'] = response.get('BTC').get('INR')
    output['ETH_USD'] = response.get('BTC').get('USD')
    output["timestamp"] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    return output
