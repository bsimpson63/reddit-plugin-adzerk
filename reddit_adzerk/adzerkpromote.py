from collections import namedtuple
import datetime
import json
import re
import string
from urllib import quote

import adzerk_api
from adzerk_utils import get_mobile_targeting_query
from pylons import request
from pylons import tmpl_context as c
from pylons import app_globals as g
import requests

from r2.controllers import api, add_controller
from r2.controllers.oauth2 import allow_oauth2_access
from r2.lib import (
    amqp,
    authorize,
    organic,
    promote,
)
from r2.lib.csrf import csrf_exempt
from r2.lib.db.sorts import epoch_seconds
from r2.lib.filters import _force_utf8
from r2.lib.pages import responsive
from r2.lib.pages.things import default_thing_wrapper
from r2.lib.template_helpers import add_sr, replace_render
from r2.lib.hooks import HookRegistrar
from r2.lib.utils import Enum, UrlParser
from r2.lib.validator import (
    nop,
    validate,
    VPrintable,
    VBoolean,
)

from r2.models import (
    Account,
    CampaignBuilder,
    FakeSubreddit,
    Frontpage,
    LinkListing,
    PromoCampaign,
    Link,
    PromotionLog,
    Subreddit,
)

from reddit_adzerk.lib.cache import PromoCampaignByFlightIdCache

hooks = HookRegistrar()

LEADERBOARD_AD_TYPE = 4
ADZERK_IMPRESSION_BUMP = 500    # add extra impressions to the number we
                                # request from adzerk in case their count
                                # is lower than our internal traffic tracking

DELCHARS = ''.join(c for c in map(chr, range(256)) if not (c.isalnum() or c.isspace()))

FREQ_CAP_TYPE = Enum(None, "hour", "day")

def sanitize_text(text):
    return _force_utf8(text).translate(None, DELCHARS)


def date_to_adzerk(d):
    utc_date = d - promote.timezone_offset
    epoch_milliseconds = int(epoch_seconds(utc_date) * 1000)
    return '/Date(%s)/' % epoch_milliseconds


def date_from_adzerk(date_str):
    try:
        epoch_str = re.findall('/Date\(([0-9]*)\)/', date_str)[0]
        epoch_milliseconds = int(epoch_str)
        epoch_seconds = epoch_milliseconds / 1000
        return datetime.datetime.fromtimestamp(epoch_seconds, tz=g.tz)
    except StandardError:
        return date_str


def render_link(link):
    return json.dumps({
        'link': link._fullname,
        'title': '',
        'author': '',
        'target': '',
    })


def update_changed(adzerk_object, **d):
    changed = [(attr, val, getattr(adzerk_object, attr, None))
               for attr, val in d.iteritems()
               if getattr(adzerk_object, attr) != val]
    if changed:
        for (attr, val, oldval) in changed:
            setattr(adzerk_object, attr, val)
        adzerk_object._send()
    return changed


def make_change_strings(changed):
    def change_to_str(change_tuple):
        attr, newval, oldval = change_tuple

        if attr in ('StartDate', 'EndDate'):
            newval = date_from_adzerk(newval)
            oldval = date_from_adzerk(oldval)

        return '%s: %s -> %s' % (attr, _force_utf8(oldval), _force_utf8(newval))

    return map(change_to_str, changed)


def update_campaign(link, az_advertiser=None):
    """Add/update a reddit link as an Adzerk Campaign"""
    if getattr(link, 'external_campaign_id', None) is not None:
        az_campaign = adzerk_api.Campaign.get(link.external_campaign_id)
    else:
        az_campaign = None

    d = {
        'SalespersonId': g.az_selfserve_salesperson_id,
        'IsDeleted': False, # deleting an adzerk object will make it
                            # unretrievable, so just set it inactive
        'IsActive': promote.is_accepted(link) and not link._deleted,
        'Price': 0,
    }

    if az_advertiser:
        d["AdvertiserId"] = az_advertiser.Id

    if az_campaign:
        changed = update_changed(az_campaign, **d)
        change_strs = make_change_strings(changed)
        if change_strs:
            log_text = 'updated %s: ' % az_campaign + ', '.join(change_strs)
        else:
            log_text = None
    else:
        d.update({
            'Name': link._fullname,
            'Flights': [],
            'StartDate': date_to_adzerk(datetime.datetime.now(g.tz)),
        })
        az_campaign = adzerk_api.Campaign.create(**d)
        link.external_campaign_id = az_campaign.Id
        link._commit()
        log_text = 'created %s' % az_campaign

    if log_text:
        PromotionLog.add(link, log_text)
        g.log.info(log_text)

    return az_campaign


def update_creative(link, az_advertiser):
    """Add/update a reddit link as an Adzerk Creative"""
    if getattr(link, 'external_creative_id', None) is not None:
        az_creative = adzerk_api.Creative.get(link.external_creative_id)
    else:
        az_creative = None

    title = link._fullname
    url = add_sr(link.url, sr_path=False) if link.is_self else link.url

    # protocols are case sensitive (lower) in adzerk.
    # can cause double protocols:
    # http://Http://www.example.com
    url = re.sub(r"^(https?)", lambda m: m.group(0).lower(), url, flags=re.I)

    # as long as there are no 3rd party trackers for the link
    # it's DNT compliant.
    DNT_compliant = (not (hasattr(link, 'third_party_tracking_url') or
        hasattr(link, 'third_party_tracking_url_2')))

    d = {
        'Body': title,
        'ScriptBody': render_link(link),
        'AdTypeId': LEADERBOARD_AD_TYPE,
        'Alt': '',
        'Url': url,
        'IsHTMLJS': True,
        'IsSync': False,
        'IsDeleted': False,
        'IsActive': not link._deleted,
        'IsNoTrack': DNT_compliant,
    }

    if az_creative:
        changed = update_changed(az_creative, **d)
        change_strs = make_change_strings(changed)
        if change_strs:
            log_text = 'updated %s: ' % az_creative + ', '.join(change_strs)
        else:
            log_text = None
    else:
        d.update({
            'AdvertiserId': az_advertiser.Id,
            'Title': title,
        })
        try:
            az_creative = adzerk_api.Creative.create(**d)
        except:
            raise ValueError(d)

        link.external_creative_id = az_creative.Id
        link._commit()
        log_text = 'created %s' % az_creative

    if log_text:
        PromotionLog.add(link, log_text)
        g.log.info(log_text)

    return az_creative


def update_advertiser(author):
    if getattr(author, 'external_advertiser_id', None) is not None:
        az_advertiser = adzerk_api.Advertiser.get(author.external_advertiser_id)
    else:
        az_advertiser = None

    if az_advertiser:
        return az_advertiser

    az_advertiser = adzerk_api.Advertiser.create(**{
        "Title": author.name,
        "IsActive": True,
        "IsDeleted": False,
    })
    author.external_advertiser_id = az_advertiser.Id
    author._commit()

    return az_advertiser


def update_flight(link, campaign, az_campaign):
    """Add/update a reddit campaign as an Adzerk Flight"""
    if getattr(campaign, 'external_flight_id', None) is not None:
        az_flight = adzerk_api.Flight.get(campaign.external_flight_id)
    else:
        az_flight = None

    campaign_overdelivered = is_overdelivered(campaign)
    delayed_start = campaign.start_date + datetime.timedelta(minutes=15)
    if delayed_start >= campaign.end_date:
        # start time must be before end time
        delayed_start = campaign.start_date

    d = {
        'StartDate': date_to_adzerk(delayed_start),
        'EndDate': date_to_adzerk(campaign.end_date),
        'OptionType': 1, # 1: CPM, 2: Remainder
        'IsUnlimited': False,
        'IsFullSpeed': False,
        'Keywords': '\n'.join(campaign.target.subreddit_names),
        'CampaignId': az_campaign.Id,
        'PriorityId': g.az_selfserve_priorities[campaign.priority_name],
        'IsDeleted': False,
        'IsActive': (not campaign.paused and
                     promote.charged_or_not_needed(campaign) and
                     not (campaign._deleted or campaign_overdelivered)),
    }

    if campaign.frequency_cap and campaign.frequency_cap_duration:
        d.update({'IsFreqCap': True,
                  'FreqCap': campaign.frequency_cap,
                  'FreqCapDuration': campaign.frequency_cap_duration,
                  'FreqCapType': FREQ_CAP_TYPE.hour})
    else:
        d['IsFreqCap'] = None

    is_cpm = hasattr(campaign, 'cpm') and campaign.priority.cpm
    if is_cpm:
        d.update({
            'Price': campaign.cpm / 100.,   # convert from cents to dollars
            'Impressions': campaign.impressions + ADZERK_IMPRESSION_BUMP,
            'GoalType': 1, # 1: Impressions
            'RateType': 2, # 2: CPM
        })
    else:
        d.update({
            'Price': campaign.bid,
            'Impressions': 100,
            'GoalType': 2, # 2: Percentage
            'RateType': 1, # 1: Flat
        })

    # Zerkel queries here
    if campaign.mobile_os:
        queries_list = []

        if 'iOS' in campaign.mobile_os:
            ios_targets = get_mobile_targeting_query(os_str='iOS',
                                                     lookup_str='modelName',
                                                     devices=campaign.ios_devices,
                                                     versions=campaign.ios_version_range)
            queries_list.append(ios_targets)

        if 'Android' in campaign.mobile_os:
            android_targets = get_mobile_targeting_query(os_str='Android',
                                                         lookup_str='formFactor',
                                                         devices=campaign.android_devices,
                                                         versions=campaign.android_version_range)
            queries_list.append(android_targets)

        if campaign.platform == 'all':
            queries_list.append('($device.formFactor CONTAINS "desktop")')

        mobile_targeting_query = ' OR '.join(queries_list)
        
        d.update({
            'CustomTargeting': mobile_targeting_query,
        })
    else:
        d.update({
            'CustomTargeting': '',
        })

    if campaign.platform != 'all':
        siteZones = []
        if campaign.platform == 'desktop':
            siteZones.append({
                'SiteId': g.az_selfserve_site_id,
                'IsExclude': False,
            })
        elif campaign.platform == 'mobile':
            siteZones.append({
                'SiteId': g.az_selfserve_mobile_web_site_id,
                'IsExclude': False,
            })

        if len(siteZones):
            d.update({
                'SiteZoneTargeting': siteZones
            })

    # special handling for location conversions between reddit and adzerk
    if campaign.location:
        campaign_country = campaign.location.country
        campaign_region = campaign.location.region
        if campaign.location.metro:
            campaign_metro = int(campaign.location.metro)
        else:
            campaign_metro = None

    if az_flight and az_flight.GeoTargeting:
        # special handling for geotargeting of existing flights
        # can't update geotargeting through the Flight endpoint, do it manually
        existing = az_flight.GeoTargeting[0]
        az_geotarget = adzerk_api.GeoTargeting._from_item(existing)

        if (campaign.location and
            (campaign_country != az_geotarget.CountryCode or
             campaign_region != az_geotarget.Region or
             campaign_metro != az_geotarget.MetroCode or
             az_geotarget.IsExclude)):
            # existing geotargeting doesn't match current location
            az_geotarget.CountryCode = campaign_country
            az_geotarget.Region = campaign_region
            az_geotarget.MetroCode = campaign_metro
            az_geotarget.IsExclude = False
            az_geotarget._send(az_flight.Id)
            log_text = 'updated geotargeting to %s' % campaign.location
            PromotionLog.add(link, log_text)
        elif not campaign.location:
            # flight should no longer be geotargeted
            az_geotarget._delete(az_flight.Id)
            log_text = 'deleted geotargeting'
            PromotionLog.add(link, log_text)

        # only allow one geotarget per flight
        for existing in az_flight.GeoTargeting[1:]:
            az_geotarget = adzerk_api.GeoTargeting._from_item(existing)
            az_geotarget._delete(az_flight.Id)

        # NOTE: need to unset GeoTargeting otherwise it will be added to the
        # flight again when we _send updates
        az_flight.GeoTargeting = None

    elif campaign.location:
        # flight endpoint works when a new flight is being created or an
        # existing one that didn't have geotargeting is being updated
        d.update({
            'GeoTargeting': [{
                'CountryCode': campaign_country,
                'Region': campaign_region,
                'MetroCode': campaign_metro,
                'IsExclude': False,
            }],
        })
    else:
        # no geotargeting, either a new flight is being created or an existing
        # flight is being updated that wasn't geotargeted
        d.update({
            'GeoTargeting': [],
        })

    if az_flight:
        changed = update_changed(az_flight, **d)
        change_strs = make_change_strings(changed)

        if campaign_overdelivered:
            billable = promote.get_billable_impressions(campaign)
            over_str = 'overdelivered %s/%s' % (billable, campaign.impressions)
            change_strs.append(over_str)

        if change_strs:
            log_text = 'updated %s: ' % az_flight + ', '.join(change_strs)
        else:
            log_text = None
    else:
        d.update({'Name': campaign._fullname})
        az_flight = adzerk_api.Flight.create(**d)
        campaign.external_flight_id = az_flight.Id
        campaign._commit()

        PromoCampaignByFlightIdCache.add(campaign)
        log_text = 'created %s' % az_flight

    if log_text:
        PromotionLog.add(link, log_text)
        g.log.info(log_text)

    if campaign_overdelivered:
        campaign.external_flight_overdelivered = True
        campaign._commit()

    return az_flight


def create_cfmap(link, campaign, az_campaign, az_creative, az_flight):
    """Create a CreativeFlightMap.

    Map the the reddit link (adzerk Creative) and reddit campaign (adzerk
    Flight).

    """

    if getattr(campaign, 'external_cfmap_id', None) is not None:
        raise AttributeError('%s has existing external_cfmap_id' % campaign)

    d = {
        'SizeOverride': False,
        'CampaignId': az_campaign.Id,
        'Percentage': 100,  # Each flight only has one creative (what about autobalanced)
        'DistributionType': 2, # 2: Percentage, 1: Auto-Balanced, 0: ???
        'Iframe': False,
        'Creative': {'Id': az_creative.Id},
        'FlightId': az_flight.Id,
        'Impressions': 100, # Percentage
        'IsDeleted': False,
        'IsActive': True,
    }

    az_cfmap = adzerk_api.CreativeFlightMap.create(az_flight.Id, **d)
    campaign.external_cfmap_id = az_cfmap.Id
    campaign._commit()

    log_text = 'created %s' % az_cfmap
    PromotionLog.add(link, log_text)
    g.log.info(log_text)

    return az_cfmap


def update_adzerk(link, campaign=None):
    g.log.debug('queuing update_adzerk %s %s' % (link, campaign))
    msg = json.dumps({
        'action': 'update_adzerk',
        'link': link._fullname,
        'campaign': campaign._fullname if campaign else None,
    })
    amqp.add_item('adzerk_q', msg)


def deactivate_orphaned_flight(az_flight_id):
    g.log.debug("queuing deactivate_orphaned_flight %d" % az_flight_id)

    amqp.add_item("adzerk_q", json.dumps({
        "action": "deactivate_orphaned_flight",
        "flight": az_flight_id,
    }))


def _update_adzerk(link, campaign):
    with g.make_lock('adzerk_update', 'adzerk-' + link._fullname):
        msg = '%s updating/creating adzerk objects for %s - %s'
        g.log.info(msg % (datetime.datetime.now(g.tz), link, campaign))
        author = Account._byID(link.author_id, data=True)
        az_advertiser = update_advertiser(author)
        az_campaign = update_campaign(link, az_advertiser)
        az_creative = update_creative(link, az_advertiser)

        # Temporarily skip new (auction/fix cpm) campaigns during
        # testing.
        if campaign and not hasattr(campaign, "bid"):
            return

        if campaign:
            az_flight = update_flight(link, campaign, az_campaign)
            if getattr(campaign, 'external_cfmap_id', None) is not None:
                az_cfmap = adzerk_api.CreativeFlightMap.get(az_flight.Id,
                                campaign.external_cfmap_id)
            else:
                az_cfmap = create_cfmap(link, campaign, az_campaign,
                                        az_creative, az_flight)
            PromotionLog.add(link, 'updated %s' % az_flight)
        else:
            PromotionLog.add(link, 'updated %s' % az_campaign)


def deactivate_overdelivered(link, campaign):
    g.log.debug('queuing deactivate_overdelivered %s %s' % (link, campaign))
    msg = json.dumps({
        'action': 'deactivate_overdelivered',
        'link': link._fullname,
        'campaign': campaign._fullname,
    })
    amqp.add_item('adzerk_q', msg)


def _deactivate_overdelivered(link, campaign):
    with g.make_lock('adzerk_update', 'adzerk-' + link._fullname):
        msg = '%s deactivating adzerk flight for %s - %s'
        g.log.info(msg % (datetime.datetime.now(g.tz), link, campaign))

        az_campaign = update_campaign(link)
        az_flight = update_flight(link, campaign, az_campaign)
        PromotionLog.add(link, 'deactivated %s' % az_flight)


def _deactivate_orphaned_flight(flight_id):
    with g.make_lock('adzerk_update', 'adzerk-%d' % flight_id):
        g.log.info('deactivating orphaned flight %d' % flight_id)

        az_flight = adzerk_api.Flight.get(flight_id)

        if not az_flight:
            return

        az_flight.IsActive = False
        az_flight._send()


@hooks.on('promote.make_daily_promotions')
def deactivate_overdelivered_campaigns(offset=0):
    for campaign, link in promote.get_scheduled_promos(offset=offset):
        if (promote.is_live_promo(link, campaign) and
                not getattr(campaign, 'external_flight_overdelivered', False) and
                is_overdelivered(campaign)):
            deactivate_overdelivered(link, campaign)


@hooks.on('promote.edit_promotion')
def edit_promotion(link):
    if not list(PromoCampaign._by_link(link._id)):
        g.log.debug("no campaigns for link, skipping %s" % link._id)
        return

    update_adzerk(link)


@hooks.on('promote.new_campaign')
def new_campaign(link, campaign):
    update_adzerk(link, campaign)


@hooks.on('promote.edit_campaign')
def edit_campaign(link, campaign):
    update_adzerk(link, campaign)


@hooks.on('promote.delete_campaign')
def delete_campaign(link, campaign):
    update_adzerk(link, campaign)


def is_overdelivered(campaign):
    if not hasattr(campaign, 'cpm') or not campaign.priority.cpm:
        return False

    billable_impressions = promote.get_billable_impressions(campaign)
    return billable_impressions >= campaign.impressions + ADZERK_IMPRESSION_BUMP


def process_adzerk():
    @g.stats.amqp_processor('adzerk_q')
    def _handle_adzerk(msg):
        data = json.loads(msg.body)
        g.log.debug('data: %s' % data)

        action = data.get('action')

        if action == 'deactivate_orphaned_flight':
            _deactivate_orphaned_flight(data['flight'])
            return

        link = Link._by_fullname(data['link'], data=True)
        if data['campaign']:
            campaign = PromoCampaign._by_fullname(data['campaign'], data=True)
        else:
            campaign = None

        if action == 'update_adzerk':
            _update_adzerk(link, campaign)
        elif action == 'deactivate_overdelivered':
            _deactivate_overdelivered(link, campaign)

    amqp.consume_items('adzerk_q', _handle_adzerk, verbose=False)

AdzerkResponse = namedtuple('AdzerkResponse',
                    ['link', 'campaign', 'target', 'imp_pixel', 'click_url'])

class AdserverResponse(object):
    def __init__(self, body):
        self.body = body


def adzerk_request(keywords, uid, num_placements=1, timeout=1.5,
                   mobile_web=False):
    placements = []
    divs = ["div%s" % i for i in xrange(num_placements)]

    if mobile_web:
        site_id = g.az_selfserve_mobile_web_site_id
    else:
        site_id = g.az_selfserve_site_id

    for div in divs:
        placement = {
          "divName": div,
          "networkId": g.az_selfserve_network_id,
          "siteId": site_id,
          "adTypes": [LEADERBOARD_AD_TYPE]
        }
        placements.append(placement)

    data = {
        "placements": placements,
        "keywords": [word.lower() for word in keywords],
        "ip": request.ip,
    }

    referrer = request.headers.get("referer", None)

    if referrer:
        data["referrer"] = referrer

    if uid:
        data["user"] = {"key": uid}

    url = 'https://%s/api/v2' % g.adzerk_engine_domain
    headers = {
        'content-type': 'application/json',
        'user-agent': request.headers.get('User-Agent'),
    }

    timer = g.stats.get_timer("providers.adzerk")
    timer.start()

    try:
        r = requests.post(url, data=json.dumps(data), headers=headers,
                          timeout=timeout)
    except (requests.exceptions.Timeout, requests.exceptions.SSLError):
        g.stats.simple_event('adzerk.request.timeout')
        return None
    except requests.exceptions.ConnectionError:
        g.stats.simple_event('adzerk.request.refused')
        return None
    finally:
        timer.stop()

    try:
        response = adzerk_api.handle_response(r)
    except adzerk_api.AdzerkError:
        g.stats.simple_event('adzerk.request.badresponse')
        g.log.error('adzerk_request: bad response (%s) %r', r.status_code,
                    r.content)
        return None

    decisions = response['decisions']

    if not decisions:
        return None

    res = []
    for div in divs:
        decision = decisions[div]
        if not decision:
            continue

        # adserver ads are not reddit links, we return the body
        if decision['campaignId'] in g.adserver_campaign_ids:
            return AdserverResponse(decision['contents'][0]['body'])

        adzerk_campaign_id = decision['campaignId']
        adzerk_flight_id = decision['flightId']
        imp_pixel = decision['impressionUrl']
        click_url = decision['clickUrl']

        campaign = PromoCampaignByFlightIdCache.get(adzerk_flight_id)

        if not campaign:
            g.stats.simple_event('adzerk.request.orphaned_flight')
            g.log.error('adzerk_request: couldn\'t find campaign for flight (az campaign: %s, flight: %s)',
                adzerk_campaign_id, adzerk_flight_id)

            # deactivate the flight, it will be reactivated if a
            # valid campaign actually exists
            deactivate_orphaned_flight(adzerk_flight_id)
            continue

        body = json.loads(decision['contents'][0]['body'])
        link = body['link']
        target = body['target']
        res.append(AdzerkResponse(link, campaign, target, imp_pixel, click_url))
    return res


@add_controller
class AdzerkApiController(api.ApiController):
    @csrf_exempt
    @allow_oauth2_access
    @validate(
        srnames=VPrintable("srnames", max_length=2100),
        is_mobile_web=VBoolean('is_mobile_web'),
        loid=nop('loid', None),
        is_refresh=VBoolean("is_refresh", default=False),
    )
    def POST_request_promo(self, srnames, is_mobile_web, loid, is_refresh):
        self.OPTIONS_request_promo()

        if not srnames:
            return

        srnames = srnames.split('+')

        # request multiple ads in case some are hidden by the builder due
        # to the user's hides/preferences
        response = adzerk_request(srnames, self.get_uid(loid),
                                  mobile_web=is_mobile_web)

        if not response:
            g.stats.simple_event('adzerk.request.no_promo')
            return

        # for adservers, adzerk returns markup so we pass it to the client
        if isinstance(response, AdserverResponse):
            g.stats.simple_event('adzerk.request.adserver')
            return responsive(response.body)

        res_by_campaign = {r.campaign: r for r in response}
        adserver_click_urls = {r.campaign: r.click_url for r in response}
        tuples = [promote.PromoTuple(r.link, 1., r.campaign) for r in response]
        builder = CampaignBuilder(tuples, wrap=default_thing_wrapper(),
                                  keep_fn=promote.promo_keep_fn,
                                  num=1,
                                  skip=True)
        listing = LinkListing(builder, nextprev=False).listing()
        promote.add_trackers(listing.things, c.site, adserver_click_urls=adserver_click_urls)
        promote.update_served(listing.things)
        if listing.things:
            g.stats.simple_event('adzerk.request.valid_promo')
            if is_refresh:
                g.stats.simple_event('adzerk.request.auto_refresh')

            w = listing.things[0]
            r = res_by_campaign[w.campaign]

            up = UrlParser(r.imp_pixel)
            up.hostname = "pixel.redditmedia.com"
            w.adserver_imp_pixel = up.unparse()
            w.adserver_click_url = r.click_url
            w.num = ""
            return responsive(w.render(), space_compress=True)
        else:
            g.stats.simple_event('adzerk.request.skip_promo')

    def get_uid(self, loid):
        if c.user_is_loggedin:
            return c.user._id36
        elif loid:
            return loid
        else:
            return None

