import json
import requests

from pylons import app_globals as g


COUNTRIES_URL = 'https://api.adzerk.net/v1/countries'
HEADERS = {
    'X-Adzerk-ApiKey': g.secrets['az_ads_key'],
    'Content-Type': 'application/x-www-form-urlencoded',
}


def get_locations(exclude_regions_without_metros=True):
    """
    Get countries/regions/metros from adzerk.

    Optionally exclude non-US regions because we can't pull inventory reports
    by region so we can't target them.

    """

    response = requests.get(COUNTRIES_URL, headers=HEADERS)

    if not (200 <= response.status_code <= 299):
        raise ValueError('response %s' % response.status_code)

    response = json.loads(response.text)
    ret = {}

    for country in response:
        country_name = country['Name']
        country_code = country['Code']
        country_regions = country['Regions']
        ret[country_code] = {
            'name': country_name,
        }

        for region in country_regions.itervalues():
            region_metros = region['Metros']

            if region_metros or not exclude_regions_without_metros:
                ret[country_code].setdefault('regions', {})
                region_code = region['Code']
                region_name = region['Name']
                country_region = {
                    'name': region_name,
                    'metros': {},
                }
                ret[country_code]['regions'][region_code] = country_region

                for metro in region_metros.itervalues():
                    metro_code = metro['Code']
                    metro_name = metro['Name']
                    country_region['metros'][metro_code] = {
                        'name': metro_name,
                    }
    return ret


def write_locations(filename):
    locations = get_locations()
    with open(filename, 'w') as f:
        f.write(json.dumps(locations, indent=2, sort_keys=True))
