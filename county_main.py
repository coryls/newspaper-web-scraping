import requests
import pandas as pd
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging
import random

logging.basicConfig(level=logging.DEBUG)

MAX_RETRIES = 10
TIMEOUT_PERIOD = 180

session = requests.Session()

retries = Retry(total=MAX_RETRIES,  # Total number of retries
                backoff_factor=1,
                # Time factor to increase between attempts. The sleep time will be: {backoff factor} * (2 ** ({number of total retries} - 1))
                status_forcelist=[429, 500, 502, 503, 504],  # Status codes to trigger a retry
                allowed_methods=frozenset(['GET', 'POST', 'PUT', 'DELETE']),  # HTTP methods to be retried
                raise_on_redirect=True,  # Retry on redirects (typically for GET requests)
                raise_on_status=True  # If True, an error is raised on the final retry attempt
                )

session.mount('https://www.newspapers.com/api/search/query', HTTPAdapter(max_retries=retries))

state_codes = ['ak', 'al', 'ar', 'az', 'ca', 'co', 'ct', 'dc', 'de', 'fl', 'ga', 'hi', 'ia', 'id',
               'il', 'in', 'ks', 'ky', 'la', 'ma', 'md', 'me', 'mi', 'mn', 'mo', 'ms', 'mt', 'nc',
               'nd', 'ne', 'nh', 'nj', 'nm', 'nv', 'ny', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd',
               'tn', 'tx', 'ut', 'va', 'vt', 'wa', 'wi', 'wv', 'wy']

state_names_dict = {'ak': 'Alaska',
                    'al': 'Alabama',
                    'ar': 'Arkansas',
                    'az': 'Arizona',
                    'ca': 'California',
                    'co': 'Colorado',
                    'ct': 'Connecticut',
                    'dc': 'District of Columbia',
                    'de': 'Delaware',
                    'fl': 'Florida',
                    'ga': 'Georgia',
                    'hi': 'Hawaii',
                    'ia': 'Iowa',
                    'id': 'Idaho',
                    'il': 'Illinois',
                    'in': 'Indiana',
                    'ks': 'Kansas',
                    'ky': 'Kentucky',
                    'la': 'Louisiana',
                    'ma': 'Massachusetts',
                    'md': 'Maryland',
                    'me': 'Maine',
                    'mi': 'Michigan',
                    'mn': 'Minnesota',
                    'mo': 'Missouri',
                    'ms': 'Mississippi',
                    'mt': 'Montana',
                    'nc': 'North Carolina',
                    'nd': 'North Dakota',
                    'ne': 'Nebraska',
                    'nh': 'New Hampshire',
                    'nj': 'New Jersey',
                    'nm': 'New Mexico',
                    'nv': 'Nevada',
                    'ny': 'New York',
                    'oh': 'Ohio',
                    'ok': 'Oklahoma',
                    'or': 'Oregon',
                    'pa': 'Pennsylvania',
                    'ri': 'Rhode Island',
                    'sc': 'South Carolina',
                    'sd': 'South Dakota',
                    'tn': 'Tennessee',
                    'tx': 'Texas',
                    'ut': 'Utah',
                    'va': 'Virginia',
                    'vt': 'Vermont',
                    'wa': 'Washington',
                    'wi': 'Wisconsin',
                    'wv': 'West Virginia',
                    'wy': 'Wyoming'}

headers = {
    'authority': 'www.newspapers.com',
    'scheme': 'https',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Encoding': 'deflate',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'Cache-Control': 'no-cache',
    'Cookie': 'YOUR_COOKIE_VALUE',
    'Pragma': 'no-cache',
    'Sec-Ch-Ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
    'Sec-Ch-Ua-Mobile': '?0',
    'Sec-Ch-Ua-Platform': '"Windows"',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36'
}

def exponential_backoff(retries):
    return min(60, (2 ** retries) + random.uniform(0, 1))

success_counts = 0

def get_data_with_backoff(url, params, max_retries=MAX_RETRIES):
    global success_counts
    retries = 0
    while retries < max_retries:
        response = session.get(url, headers=headers, params=params)
        if response.status_code == 200:
            success_counts += 1
            time.sleep(0.5)
            return response.json()
        elif response.status_code == 429:  # Too Many Requests
            success_counts = 0
            wait_time = exponential_backoff(retries)
            logging.debug(f'Status code 429: waiting {wait_time} seconds')
            time.sleep(wait_time)
            retries += 1
        else:
            success_counts = 0
            logging.debug(response.status_code)
            retries += 1
            time.sleep(TIMEOUT_PERIOD)
    raise Exception("Max retries exceeded")

def get_counties(start_date, end_date, keyword, state):
    url = 'https://www.newspapers.com/api/search/query'
    params = {
        'product': 1,
        'entity-types': 'page,',
        'start': '*',
        'count': 100,
        'region': f'us-{state}',
        'keyword': keyword,
        'date-start': start_date,
        'date-end': end_date,
        'facet-year': 1000,
        'facet-country': 200,
        'facet-region': 300,
        'facet-county': 260,
        'facet-city': 150,
        'facet-entity': 5,
        'facet-publication': 5,
        'include-publication-metadata': 'true'
    }
    return get_data_with_backoff(url, params), url, params

def get_city(url, params, county):
    params = {
        **params,
        'county': county
    }
    return get_data_with_backoff(url, params).get('facets', {}).get('city', [])

def main(start_year, end_year, keyword):
    whole_df = pd.DataFrame()
    for count, state in enumerate(state_codes):
        data, url, params = get_counties(start_year, end_year, keyword, state)
        if data.get('recordCount', 0) > 0:
            county_dict = data['facets']['county']
            counties = [item['value'] for item in county_dict]
            df = pd.DataFrame(columns=['state', 'county', 'value', 'count'])
            for county in counties:
                city_data = get_city(url, params, county)
                df_data = pd.DataFrame(city_data)
                df_data['county'] = county
                df = pd.concat([df, df_data], ignore_index=True)
            df['state'] = state_names_dict[state]
            whole_df = pd.concat([whole_df, df], ignore_index=True)
        logging.info(f'{count}/{len(state_codes) - 1} completed')
    whole_df = whole_df.rename(columns={'value': 'city'})
    whole_df.to_excel(f'result_{start_year}_to_{end_year}.xlsx')
    logging.info('Data successfully saved to Excel file')

if __name__ == "__main__":
    start_date = int(input('Please select a start date (year): '))
    end_date = int(input('Please select an end date (year): '))
    keyword = str(input('Please provide a keyword: '))
    main(start_date, end_date, keyword)
