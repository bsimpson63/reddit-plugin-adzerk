from collections import namedtuple
import datetime
import json
import string
from urllib import quote

import adzerk_api
from pylons import c, g
import requests

from r2.controllers import api, add_controller
from r2.lib import (
    amqp,
    authorize,
    organic,
    promote,
)
from r2.lib.db.sorts import epoch_seconds
from r2.lib.filters import spaceCompress, _force_utf8
from r2.lib.pages.things import default_thing_wrapper
from r2.lib.template_helpers import replace_render
from r2.lib.hooks import HookRegistrar
from r2.lib.validator import (
    validate,
    VPrintable,
)

from r2.models import (
    Account,
    CampaignBuilder,
    FakeSubreddit,
    Frontpage,
    Link,
    LinkListing,
    PromoCampaign,
    PromotionLog,
    Subreddit,
)


hooks = HookRegistrar()

ADZERK_IMPRESSION_BUMP = 500    # add extra impressions to the number we
                                # request from adzerk in case their count
                                # is lower than our internal traffic tracking

DELCHARS = ''.join(c for c in map(chr, range(256)) if not (c.isalnum() or c.isspace()))

def sanitize_text(text):
    return _force_utf8(text).translate(None, DELCHARS)


def date_to_adzerk(d):
    utc_date = d - promote.timezone_offset
    epoch_milliseconds = int(epoch_seconds(utc_date) * 1000)
    return '/Date(%s)/' % epoch_milliseconds


def srname_to_keyword(srname):
    return srname or Frontpage.name


def render_link(link, campaign):
    author = Account._byID(link.author_id, data=True)
    return json.dumps({
        'link': link._fullname,
        'campaign': campaign._fullname,
        'title': '',
        'author': '',
        'target': campaign.sr_name,
    })


def update_changed(adzerk_object, **d):
    changed = [(attr, val) for attr, val in d.iteritems()
                          if getattr(adzerk_object, attr) != val]
    if changed:
        for (attr, val) in changed:
            setattr(adzerk_object, attr, val)
        adzerk_object._send()
    return changed


def update_campaign(link):
    """Add/update a reddit link as an Adzerk Campaign"""
    if hasattr(link, 'adzerk_campaign_id'):
        az_campaign = adzerk_api.Campaign.get(link.adzerk_campaign_id)
    else:
        az_campaign = None

    d = {
        'AdvertiserId': g.az_selfserve_advertiser_id,
        'IsDeleted': link._deleted,
        'IsActive': promote.is_accepted(link),
        'Price': 0,
    }

    log_text = None
    if az_campaign:
        changed = update_changed(az_campaign, **d)
    else:
        d.update({
            'Name': link._fullname,
            'Flights': [],
            'StartDate': date_to_adzerk(datetime.datetime.now(g.tz)),
        })
        az_campaign = adzerk_api.Campaign.create(**d)
        link.adzerk_campaign_id = az_campaign.Id
        link._commit()
        log_text = 'created %s' % az_campaign

    if log_text:
        PromotionLog.add(link, log_text)
        g.log.info(log_text)

    return az_campaign


def update_creative(link, campaign):
    """Add/update a reddit link/campaign as an Adzerk Creative"""
    if hasattr(campaign, 'adzerk_creative_id'):
        az_creative = adzerk_api.Creative.get(campaign.adzerk_creative_id)
    else:
        az_creative = None

    title = '-'.join((link._fullname, campaign._fullname))
    d = {
        'Body': title,
        'ScriptBody': render_link(link, campaign),
        'AdvertiserId': g.az_selfserve_advertiser_id,
        'AdTypeId': g.az_selfserve_ad_type,
        'Alt': '',
        'Url': '',
        'IsHTMLJS': True,
        'IsSync': False,
        'IsDeleted': campaign._deleted,
        'IsActive': not campaign._deleted,
    }

    log_text = None
    if az_creative:
        changed = update_changed(az_creative, **d)
    else:
        d.update({'Title': title})
        try:
            az_creative = adzerk_api.Creative.create(**d)
        except:
            raise ValueError(d)

        campaign.adzerk_creative_id = az_creative.Id
        campaign._commit()
        log_text = 'created %s' % az_creative

    if log_text:
        PromotionLog.add(link, log_text)
        g.log.info(log_text)

    return az_creative


def update_flight(link, campaign):
    """Add/update a reddit campaign as an Adzerk Flight"""
    if hasattr(campaign, 'adzerk_flight_id'):
        az_flight = adzerk_api.Flight.get(campaign.adzerk_flight_id)
    else:
        az_flight = None

    az_campaign = adzerk_api.Campaign.get(link.adzerk_campaign_id)

    d = {
        'StartDate': date_to_adzerk(campaign.start_date),
        'EndDate': date_to_adzerk(campaign.end_date),
        'OptionType': 1, # 1: CPM, 2: Remainder
        'IsUnlimited': False,
        'IsFullSpeed': False,
        'Keywords': srname_to_keyword(campaign.sr_name),
        'CampaignId': az_campaign.Id,
        'PriorityId': g.az_selfserve_priorities[campaign.priority_name],
        'IsDeleted': campaign._deleted,
        'IsActive': (promote.charged_or_not_needed(campaign) and
                     not (campaign._deleted or is_overdelivered(campaign))),
        'IsFreqCap': None,
    }

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

    log_text = None
    if az_flight:
        changed = update_changed(az_flight, **d)
    else:
        d.update({'Name': campaign._fullname})
        az_flight = adzerk_api.Flight.create(**d)
        campaign.adzerk_flight_id = az_flight.Id
        campaign._commit()
        log_text = 'created %s' % az_flight

    if log_text:
        PromotionLog.add(link, log_text)
        g.log.info(log_text)

    return az_flight


def update_cfmap(link, campaign):
    """Add/update a CreativeFlightMap.
    
    Map the the reddit link (adzerk Creative) and reddit campaign (adzerk
    Flight).

    """

    az_campaign = adzerk_api.Campaign.get(link.adzerk_campaign_id)
    az_creative = adzerk_api.Creative.get(campaign.adzerk_creative_id)
    az_flight = adzerk_api.Flight.get(campaign.adzerk_flight_id)

    if hasattr(campaign, 'adzerk_cfmap_id'):
        az_cfmap = adzerk_api.CreativeFlightMap.get(az_flight.Id,
                                                    campaign.adzerk_cfmap_id)
    else:
        az_cfmap = None

    d = {
        'SizeOverride': False,
        'CampaignId': az_campaign.Id,
        'PublisherAccountId': g.az_selfserve_advertiser_id,
        'Percentage': 100,  # Each flight only has one creative (what about autobalanced)
        'DistributionType': 2, # 2: Percentage, 1: Auto-Balanced, 0: ???
        'Iframe': False,
        'Creative': {'Id': az_creative.Id},
        'FlightId': az_flight.Id,
        'Impressions': 100, # Percentage
        'IsDeleted': campaign._deleted,
        'IsActive': not campaign._deleted,
    }


    log_text = None
    if az_cfmap:
        changed = update_changed(az_cfmap, **d)
    else:
        az_cfmap = adzerk_api.CreativeFlightMap.create(az_flight.Id, **d)
        campaign.adzerk_cfmap_id = az_cfmap.Id
        campaign._commit()
        log_text = 'created %s' % az_cfmap

    if log_text:
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


def _update_adzerk(link, campaign):
    with g.make_lock('adzerk_update', 'adzerk-' + link._fullname):
        msg = '%s updating/creating adzerk objects for %s - %s'
        g.log.info(msg % (datetime.datetime.now(g.tz), link, campaign))
        az_campaign = update_campaign(link)

        if campaign:
            az_creative = update_creative(link, campaign)
            az_flight = update_flight(link, campaign)
            az_cfmap = update_cfmap(link, campaign)
            PromotionLog.add(link, 'updated %s' % az_flight)
        else:
            PromotionLog.add(link, 'updated %s' % az_campaign)


@hooks.on('promote.make_daily_promotions')
def deactivate_oversold(offset=0):
    for campaign, link in promote.get_scheduled_promos(offset=offset):
        if promote.is_live_promo(link) and is_overdelivered(campaign):
            update_adzerk(link, campaign)


@hooks.on('promote.new_promotion')
def new_promotion(link):
    update_adzerk(link)


@hooks.on('promote.edit_promotion')
def edit_promotion(link):
    update_adzerk(link)


def _deactivate_link(link):
    with g.make_lock('adzerk_update', 'adzerk-' + link._fullname):
        g.log.debug('running deactivate_link %s' % link)
        az_campaign = adzerk_api.Campaign.get(link.adzerk_campaign_id)
        az_campaign.IsActive = False
        az_campaign._send()
        PromotionLog.add(link, 'deactivated %s' % az_campaign)


@hooks.on('promote.new_campaign')
def new_campaign(link, campaign):
    update_adzerk(link, campaign)


@hooks.on('promote.edit_campaign')
def edit_campaign(link, campaign):
    update_adzerk(link, campaign)


def _deactivate_campaign(link, campaign):
    with g.make_lock('adzerk_update', 'adzerk-' + link._fullname):
        g.log.debug('running deactivate_campaign %s' % link)
        az_flight = adzerk_api.Flight.get(campaign.adzerk_flight_id)
        if az_flight.IsActive:
            az_flight.IsActive = False
            az_flight._send()
            PromotionLog.add(link, 'deactivated %s' % az_flight)


@hooks.on('promote.delete_campaign')
def delete_campaign(link, campaign):
    update_adzerk(link, campaign)


def is_overdelivered(campaign):
    if not hasattr(campaign, 'cpm') or not campaign.priority.cpm:
        return False

    billable_impressions = promote.get_billable_impressions(campaign)
    return billable_impressions >= campaign.impressions


def process_adzerk():
    @g.stats.amqp_processor('adzerk_q')
    def _handle_adzerk(msg):
        data = json.loads(msg.body)
        g.log.debug('data: %s' % data)
        action = data.get('action')
        if action == 'deactivate_link':
            link = Link._by_fullname(data['link'], data=True)
            _deactivate_link(link)
        elif action == 'deactivate_campaign':
            link = Link._by_fullname(data['link'], data=True)
            campaign = PromoCampaign._by_fullname(data['campaign'], data=True)
            _deactivate_campaign(link, campaign)
        elif action == 'update_adzerk':
            link = Link._by_fullname(data['link'], data=True)
            if data['campaign']:
                campaign = PromoCampaign._by_fullname(data['campaign'],
                                                      data=True)
            else:
                campaign = None
            _update_adzerk(link, campaign)
    amqp.consume_items('adzerk_q', _handle_adzerk, verbose=False)

AdzerkResponse = namedtuple('AdzerkResponse',
                    ['link', 'campaign', 'target', 'imp_pixel', 'click_url'])

def adzerk_request(keywords, num_placements=1, timeout=1.5):
    placements = []
    divs = ["div%s" % i for i in xrange(num_placements)]
    for div in divs:
        placement = {
          "divName": div,
          "networkId": g.az_selfserve_network_id,
          "siteId": g.az_selfserve_site_id,
          "adTypes": [g.az_selfserve_ad_type]
        }
        placements.append(placement)

    data = {
        "placements": placements,
        "keywords": [word.lower() for word in keywords],
    }

    url = 'https://engine.adzerk.net/api/v2'
    headers = {'content-type': 'application/json'}

    timer = g.stats.get_timer("adzerk_timer")
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

    response = json.loads(r.text)
    decisions = response['decisions']

    if not decisions:
        return None

    res = []
    for div in divs:
        decision = decisions[div]
        if not decision:
            continue

        imp_pixel = decision['impressionUrl']
        click_url = decision['clickUrl']
        body = json.loads(decision['contents'][0]['body'])
        campaign = body['campaign']
        link = body['link']
        target = body['target']
        res.append(AdzerkResponse(link, campaign, target, imp_pixel, click_url))
    return res


@add_controller
class AdzerkApiController(api.ApiController):
    @validate(srnames=VPrintable("srnames", max_length=2100))
    def POST_request_promo(self, srnames):
        if not srnames:
            return

        srnames = srnames.split('+')

        # request multiple ads in case some are hidden by the builder due
        # to the user's hides/preferences
        response = adzerk_request(srnames)

        if not response:
            g.stats.simple_event('adzerk.request.no_promo')
            return

        res_by_campaign = {r.campaign: r for r in response}
        tuples = [promote.PromoTuple(r.link, 1., r.campaign) for r in response]
        builder = CampaignBuilder(tuples, wrap=default_thing_wrapper(),
                                  keep_fn=promote.is_promoted,
                                  num=1,
                                  skip=True)
        listing = LinkListing(builder, nextprev=False).listing()
        if listing.things:
            g.stats.simple_event('adzerk.request.valid_promo')
            w = listing.things[0]
            r = res_by_campaign[w.campaign]
            w.adserver_imp_pixel = r.imp_pixel
            w.adserver_click_url = r.click_url
            return spaceCompress(w.render())
        else:
            g.stats.simple_event('adzerk.request.skip_promo')
