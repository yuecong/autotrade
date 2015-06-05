from __future__ import print_function
import logging
import requests,json

log = logging.getLogger(__name__)
print = log.info
logging.getLogger("requests").setLevel(logging.WARNING)
logging.basicConfig(level = logging.INFO,format = '%(asctime)s [%(levelname)s] %(message)s', datefmt = '%Y-%m-%d %H:%M:%S')

PRIVATE_KEY = 'ffcf6286cb14d46ea3c14f659f8b5d14-3ab610cb0f3a4e6494362960ab73d04f'
AUTH_TOKEN = {'Authorization':'Bearer '+ PRIVATE_KEY}

def get_account_info():
    url = 'https://api-fxpractice.oanda.com/v1/accounts'
    output = requests.get(url, headers = AUTH_TOKEN)
    return output.text

if __name__ == '__main__':
    print("Start simple automatic trading tool...")
    account_info = get_account_info()
    print(account_info)
