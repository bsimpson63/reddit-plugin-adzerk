from collections import defaultdict, namedtuple
import datetime
import json
import requests

from pylons import g

from reddit_adzerk.adzerkads import FRONTPAGE_NAME
from r2.models.promo import Location
from r2.models.promo_metrics import LocationPromoMetrics
from r2.models.subreddit import Frontpage

# https://github.com/adzerk/adzerk-api/wiki/Reporting-API

REPORT_URL = 'https://api.adzerk.net/v1/report'
HEADERS = {
    'X-Adzerk-ApiKey': g.secrets['az_ads_key'],
    'Content-Type': 'application/x-www-form-urlencoded',
}


ReportItem = namedtuple('ReportItem', ['start', 'end', 'impressions', 'clicks'])
ReportTuple = namedtuple('ReportTuple', ['date', 'impressions', 'clicks'])

AZ_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


def az_to_date(date_str):
    return datetime.datetime.strptime(date_str, AZ_DATE_FORMAT).date()


def case_insensitive_get(d, key):
    # adzerk isn't consistent with casing
    mapping = {k.lower(): k for k in d.keys()}
    key = mapping[key.lower()]
    return d[key]


def mangle_frontpage_name(keyword):
    if keyword == Frontpage.name:
        return FRONTPAGE_NAME
    else:
        return keyword


def demangle_frontpage_name(keyword):
    if keyword == FRONTPAGE_NAME:
        return Frontpage.name
    else:
        return keyword


def get_report(start, end, date_grouping='day', additional_groups=None,
               filters=None):
    additional_groups = additional_groups or []
    groups = [date_grouping] + additional_groups
    filters = filters or []

    data = {
        'StartDate': start.strftime('%m/%d/%Y'),
        'EndDate': end.strftime('%m/%d/%Y'),
        'GroupBy': groups,
        'Parameters': filters,
    }

    criteria = "criteria=%s" % json.dumps(data)

    response = requests.post(REPORT_URL, headers=HEADERS, data=criteria)

    if not (200 <= response.status_code <= 299):
        raise ValueError('response %s' % response.status_code)

    az_report = json.loads(response.text)
    records_by_date = az_report['Records']
    report = defaultdict(list) # branch by group then list by date

    for records in records_by_date:
        start = az_to_date(records['FirstDate'])
        end = az_to_date(records['LastDate'])
        records_by_group = records['Details']
        for by_group in records_by_group:
            group = tuple(case_insensitive_get(by_group['Grouping'], name)
                          for name in additional_groups)
            impressions = by_group['Impressions']
            clicks = by_group['Clicks']
            item = ReportItem(start=start, end=end, impressions=impressions,
                              clicks=clicks)
            report[group].append(item)
    return report


def get_location_report(start, end, location_scope=None, keywords=None):
    groups = ['keyword']
    if location_scope:
        groups.append(location_scope)

    keywords = keywords or [Frontpage.name] # default to frontpage
    keywords = map(mangle_frontpage_name, keywords)
    filters = [{'keyword': keyword} for keyword in keywords]
    basic_report = get_report(start, end, date_grouping='day',
                              additional_groups=groups, filters=filters)

    # put the report into a nicer format
    report = defaultdict(dict)
    for group, items in basic_report.iteritems():
        keyword = demangle_frontpage_name(group[0])

        # convert to list of ReportTuples
        items = [ReportTuple(item.start, item.impressions, item.clicks)
                 for item in items]

        if location_scope:
            location = group[1]
            report[keyword][location] = items
        else:
            report[keyword] = items
    return report


def get_location_inventory():
    now = datetime.datetime.now(g.tz)
    end = (now - datetime.timedelta(days=2)).date()
    start = end - datetime.timedelta(days=14)

    keywords = [Frontpage.name]
    base_report = get_location_report(start, end, keywords=keywords)
    country_report = get_location_report(start, end, keywords=keywords,
                                         location_scope='countryCode')
    metro_report = get_location_report(start, end, keywords=keywords,
                                       location_scope='metroCode')

    # construct metro to region mapping
    metro_to_region = {}
    for region_code, region in g.locations['US']['regions'].iteritems():
        for metro_code in region['metros']:
            metro_to_region[metro_code] = region_code

    # construct location to impressions
    ret = []

    location = Location(None)
    impressions = min(t.impressions for t in base_report[Frontpage.name])
    ret.append((location, Frontpage, impressions))

    for country_code, report in country_report[Frontpage.name].iteritems():
        if country_code == 'None':
            continue

        location = Location(country_code)
        impressions = min(t.impressions for t in report)
        ret.append((location, Frontpage, impressions))

    for metro_code, report in metro_report[Frontpage.name].iteritems():
        metro_code = str(metro_code)
        region_code = metro_to_region[metro_code]
        location = Location('US', region=region_code, metro=metro_code)
        impressions = min(t.impressions for t in report)
        ret.append((location, Frontpage, impressions))

    return ret


def write_location_inventory():
    location_inventory = get_location_inventory()
    LocationPromoMetrics.set(location_inventory)
