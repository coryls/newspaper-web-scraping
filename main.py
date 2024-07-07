import requests
import pandas as pd
import time
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import logging


logging.basicConfig(level=logging.DEBUG)


MAX_RETRIES = 3
TIMEOUT_PERIOD = 180





# Create a session
session = requests.Session()

# Define retry behavior
retries = Retry(total=10,  # Total number of retries
                backoff_factor=1,
                # Time factor to increase between attempts. The sleep time will be: {backoff factor} * (2 ** ({number of total retries} - 1))
                status_forcelist=[429, 500, 502, 503, 504],  # Status codes to trigger a retry
                allowed_methods=frozenset(['GET', 'POST', 'PUT', 'DELETE']),  # HTTP methods to be retried
                raise_on_redirect=True,  # Retry on redirects (typically for GET requests)
                raise_on_status=True  # If True, an error is raised on the final retry attempt
                )

# Mount the HTTPAdapter to the session
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



# headers for http
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


def get_counties(start_date, end_date, keyword, state):

    """
    Fetch the county data based on the provided parameters.

    Args:
    - start_date (int): Start date for the search.
    - end_date (int): End date for the search.
    - keyword (str): Keyword for the search.
    - state (str): State code for the search.

    Returns:
    - JSON response and the URL used for the request or None if unsuccessful.
    """


    #url = f'https://www.newspapers.com/api/search/query?product=1&entity-types=page,marriage,obituary,enslavement&start=*&count=100&region=us-{state}&keyword={keyword}&date-start={start_date}&date-end={end_date}&facet-year=1000&facet-country=200&facet-region=300&facet-county=260&facet-city=150&facet-entity=5&facet-publication=5&include-publication-metadata=true'
    url = f'https://www.newspapers.com/api/search/query?product=1&entity-types=page,&start=*&count=100&region=us-{state}&keyword={keyword}&date-start={start_date}&date-end={end_date}&facet-year=1000&facet-country=200&facet-region=300&facet-county=260&facet-city=150&facet-entity=5&facet-publication=5&include-publication-metadata=true'


    COUNTER = 0
    while COUNTER != MAX_RETRIES:

        time.sleep(1)
        response = session.get(url, headers=headers)


        if response.status_code == 200:

            r = response.json()
            return r, url
        elif response.status_code == 429:

            logging.debug(f'status code is 429 {TIMEOUT_PERIOD/60} minute rest period')
            COUNTER += 1
            time.sleep(TIMEOUT_PERIOD)

        else:
            logging.debug(response.status_code)
            COUNTER += 1
            time.sleep(TIMEOUT_PERIOD)

    return None






def get_city(url, county):
    """
    Fetch city data for a specific county.

    Args:
    - url (str): Base URL for the request.
    - county (str): County name.

    Returns:
    - List of cities or None if unsuccessful.
    """

    url += f'&county={county}'

    COUNTER = 0

    while COUNTER != MAX_RETRIES:

        time.sleep(1)
        response = session.get(url, headers=headers)


        if response.status_code == 200:

            r = response.json()

            cities = r['facets']['city']

            return (cities)

        elif response.status_code == 429:

            logging.debug(f'status code is 429 {TIMEOUT_PERIOD/60} minute rest period')

            COUNTER += 1
            time.sleep(TIMEOUT_PERIOD)

        else:
            logging.debug(response.status_code)
            COUNTER += 1
            time.sleep(TIMEOUT_PERIOD)
    return None










def main(start_year, end_year, keyword):

    """
    Main function to retrieve county and city data for a range of years and a keyword.
    The results are saved to an Excel file.

    Args:
    - start_year (int): Starting year.
    - end_year (int): Ending year.
    - keyword (str): Keyword for the search.
    """

    whole_df = pd.DataFrame()

    for count,state in enumerate(state_codes):



        data, url = get_counties(start_year, end_year, keyword, state)





        if data['recordCount'] > 0:          #if a state has 0 data for all its counties skip to the next state
            county_dict = data['facets']['county']

            counties = [item['value'] for item in county_dict]  # creats a list of all the counties that contain data for the state
            df = pd.DataFrame(columns=['state', 'county', 'value', 'count'])
            for county in counties:
                city_data = get_city(url, county)  #get data for each city inside county

                df_data = pd.DataFrame(city_data)
                df_data['county'] = county
                df = pd.concat([df, df_data], ignore_index=True)
            df['state'] = state_names_dict[state]
            whole_df = pd.concat([whole_df, df], ignore_index=True)

        logging.info(f'{count}/{len(state_codes)-1} completed')

    whole_df = whole_df.rename(columns={'value':'city'})
    #whole_df.to_excel(f'{keyword}_{start_year}_to_{end_year}.xlsx')
    whole_df.to_excel(f'result_{start_year}_to_{end_year}.xlsx')

    logging.info('data successfully saved to excel file')

if __name__ == "__main__":

    start_date = int(input('Please select a start date(year) '))
    end_date = int(input('Please select an end date (year) '))
    keyword = str(input('Please provide a key word '))


    main(start_date, end_date, keyword)