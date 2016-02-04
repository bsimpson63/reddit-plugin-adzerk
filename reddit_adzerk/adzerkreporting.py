"""
Generate and processes daily/lifetime reports for ad campaigns.

Runs link reports by day, by campaign, and stores in traffic db.
Runs campaign reports for lifetime impression, clicks, and spend.

A cron job is used to queue reports for promos that are currently
serving, or served the day before.  The queue blocks until a
report can be retrived until moving on to the next item.  If a
report is pending for more than `az_reporting_timeout` it is
assumed to have failed and is generated.
"""

import itertools
import json
import math
import pytz
import time
from collections import defaultdict
from datetime import datetime, timedelta

from dateutil.parser import parse as parse_date
from pylons import app_globals as g
from sqlalchemy.orm import scoped_session, sessionmaker

from r2.lib import (
    amqp,
    promote,
)
from r2.models import (
    Link,
    PromoCampaign,
)
from r2.models.traffic import (
    engine,
    AdserverClickthroughsByCodename,
    AdserverImpressionsByCodename,
    AdserverSpentPenniesByCodename,
    AdserverTargetedClickthroughsByCodename,
    AdserverTargetedImpressionsByCodename,
    AdserverTargetedSpentPenniesByCodename,
)

from reddit_adzerk import (
    adzerk_api,
    report,
)

RETRY_SLEEP_SECONDS = 3

Session = scoped_session(sessionmaker(bind=engine))


def queue_promo_reports():
    """
    Queue reports for promos that are currently
    serving, or served the day before.
    """
    prev_promos = promote.get_served_promos(offset=-1)
    promos = promote.get_served_promos(offset=0)
    already_processed_links = set()
    already_processed_campaigns = set()

    for campaign, link in itertools.chain(prev_promos, promos):
        if link._id36 not in already_processed_links:
            _generate_link_report(link)
            already_processed_links.add(link._id36)

        if campaign._id36 not in already_processed_campaigns:
            _generate_promo_report(campaign)
            already_processed_campaigns.add(campaign._id36)

    amqp.worker.join()


def _generate_link_report(link):
    g.log.info("queuing report for link %s" % link._fullname)
    amqp.add_item("adzerk_reporting_q", json.dumps({
        "action": "generate_daily_link_report",
        "link_id": link._id,
    }))


def _generate_promo_report(campaign):
    g.log.info("queuing report for campaign %s" % campaign._fullname)
    amqp.add_item("adzerk_reporting_q", json.dumps({
        "action": "generate_lifetime_campaign_report",
        "campaign_id": campaign._id,
    }))


def _normalize_usage(impressions, clicks, spent):
    # adzerk processes clicks faster than impressions
    # throw away results that are obviously wrong.
    if clicks > impressions:
        impressions = 0
        clicks = 0
        spent = 0

    return (impressions, clicks, spent)


def _get_total_impressions(report_fragment):
    return report_fragment.get("TotalImpressions", 0)


def _get_total_clicks(report_fragment):
    return (report_fragment.get("TotalClicks", 0) -
        report_fragment.get("TotalFraudulentClicks", 0))


def _get_total_spent(report_fragment):
    return report_fragment.get("TotalTrueRevenue", 0)


def _get_total_usage(report_fragment):
    impressions = _get_total_impressions(report_fragment)
    clicks = _get_total_clicks(report_fragment)
    spent = _get_total_spent(report_fragment)

    return _normalize_usage(impressions, clicks, spent)


def _get_impressions(report_fragment):
    return report_fragment.get("Impressions", 0)


def _get_clicks(report_fragment):
    return (report_fragment.get("Clicks", 0) -
        report_fragment.get("FraudulentClicks", 0))


def _get_spent(report_fragment):
    return report_fragment.get("TrueRevenue", 0)


def _get_usage(report_fragment):
    impressions = _get_impressions(report_fragment)
    clicks = _get_clicks(report_fragment)
    spent = _get_spent(report_fragment)

    return _normalize_usage(impressions, clicks, spent)

def _get_date(report_fragment):
    date = report_fragment.get("Date")

    if not date:
        return None

    return parse_date(date)


def _get_fullname(cls, report_fragment):
    fullname = report_fragment.get("Title", "")

    if not fullname.startswith(cls._fullname_prefix):
        return None
    else:
        return fullname

def _get_flight_id(report_fragment):
    return report_fragment.get("Grouping", {}).get("OptionId", None)


def _handle_generate_daily_link_report(link_id):
    now = datetime.utcnow()
    link = Link._byID(link_id, data=True)
    campaigns = list(PromoCampaign._by_link(link._id))

    if not campaigns:
        return

    link_start = min([promo.start_date for promo in campaigns])
    link_end = max([promo.end_date for promo in campaigns])

    now = now.replace(tzinfo=pytz.utc)
    link_start = link_start.replace(tzinfo=pytz.utc)
    link_end = link_end.replace(tzinfo=pytz.utc)

    # if data has already been processed then there's no need
    # to redo it.  use the last time the report was run as a 
    # starting point, but subtract 24hrs since initial numbers
    # are preliminary.
    if hasattr(link, "last_daily_report_run"):
        start = max([
            link.last_daily_report_run - timedelta(hours=24),
            link_start,
        ])

        # in cases where we may be running a report well after a link
        # has completed ensure we always use the actual start.
        if start > link_end:
            start = link_start

    else:
        start = link_start

    end = min([now, link_end])

    g.log.info("generating report for link %s" % link._fullname)

    report_id = report.queue_report(
        start=start,
        end=end,
        groups=["optionId", "day"],
        parameters=[{
            "campaignId": link.external_campaign_id,
        }],
    )

    g.log.info("processing report for link (%s/%s)" %
        (link._fullname, report_id))

    try:
        _process_daily_link_report(
            link=link,
            report_id=report_id,
            queued_date=now,
        )

        g.log.info("successfully processed report for link (%s/%s)" %
            (link._fullname, report_id))
    except report.ReportFailedException as e:
        g.log.error(e)
        # retry if report failed
        _generate_link_report(link)


def _handle_generate_lifetime_campaign_report(campaign_id):
    now = datetime.utcnow()
    campaign = PromoCampaign._byID(campaign_id, data=True)
    start = campaign.start_date.replace(tzinfo=pytz.utc)
    end = campaign.end_date.replace(tzinfo=pytz.utc)
    now = now.replace(tzinfo=pytz.utc)

    end = min([now, end])

    g.log.info("generating report for campaign %s" % campaign._fullname)

    report_id = report.queue_report(
        start=start,
        end=end,
        parameters=[{
            "flightId": campaign.external_flight_id,
        }],
    )

    try:
        _process_lifetime_campaign_report(
            campaign=campaign,
            report_id=report_id,
            queued_date=now,
        )

        g.log.info("successfully processed report for campaign (%s/%s)" %
            (campaign._fullname, report_id))
    except report.ReportFailedException as e:
        g.log.error(e)
        # retry if report failed
        _generate_promo_report(campaign)


def _process_lifetime_campaign_report(campaign, report_id, queued_date):
    """
    Processes report for the lifetime of the campaign.

    Exponentially backs off on retries, throws on timeout.
    """

    attempt = 1

    while True:
        try:
            report_result = report.fetch_report(report_id)
            break
        except report.ReportPendingException as e:
            timeout = (datetime.utcnow().replace(tzinfo=pytz.utc) -
                timedelta(seconds=g.az_reporting_timeout))

            if queued_date < timeout:
                raise report.ReportFailedException("campign report timed out (%s/%s)" %
                    (campaign._fullname, report_id))
            else:
                sleep_time = math.pow(RETRY_SLEEP_SECONDS, attempt)
                attempt = attempt + 1

                g.log.warning("campaign report still pending, retrying in %d seconds (%s/%s)" %
                    (RETRY_SLEEP_SECONDS, campaign._fullname, report_id))

                time.sleep(sleep_time)

    impressions, clicks, spent = _get_total_usage(report_result)

    campaign.adserver_spent_pennies = int(spent * 100)
    campaign.adserver_impressions = impressions
    campaign.adserver_clicks = clicks
    campaign.last_lifetime_report = report_id
    campaign.last_lifetime_report_run = queued_date

    campaign._commit()


def _reporting_factory():
    return dict(
        impressions=0,
        clicks=0,
        spent_pennies=0,
    )

def _process_daily_link_report(link, report_id, queued_date):
    """
    Processes report grouped by day and flight.

    Exponentially backs off on retries, throws on timeout.
    """

    attempt = 1

    while True:
        try:
            report_result = report.fetch_report(report_id)
            break
        except report.ReportPendingException as e:
            timeout = (datetime.utcnow().replace(tzinfo=pytz.utc) -
                timedelta(seconds=g.az_reporting_timeout))

            if queued_date < timeout:
                raise report.ReportFailedException("link report timed out (%s/%s)" %
                    (link._fullname, report_id))
            else:
                sleep_time = math.pow(RETRY_SLEEP_SECONDS, attempt)
                attempt = attempt + 1

                g.log.warning("link report still pending, retrying in %d seconds (%s/%s)" %
                    (RETRY_SLEEP_SECONDS, link._fullname, report_id))

                time.sleep(sleep_time)

    g.log.debug(report_result)

    campaigns_by_fullname = {campaign._fullname: campaign for campaign
        in PromoCampaign._by_link(link._id)}

    # report is by date, by flight. each record is a day
    # and each detail is a flight for that day.
    for record in report_result.get("Records", []):
        impressions, clicks, spent = _get_usage(record)
        date = _get_date(record)

        _insert_daily_link_reporting(
            codename=link._fullname,
            date=date,
            impressions=impressions,
            clicks=clicks,
            spent_pennies=spent * 100.,
        )

        campaign_details = defaultdict(_reporting_factory)
        for detail in record.get("Details", []):
            campaign_fullname = _get_fullname(PromoCampaign, detail)

            if not campaign_fullname:
                g.log.error("invalid fullname for campaign (%s/%s)" %
                    (campaign_fullname, flight_id))
                continue

            campaign = campaigns_by_fullname.get(campaign_fullname)

            if not campaign:
                flight_id = _get_flight_id(detail)
                g.log.warning("no campaign for flight (%s/%s)" %
                    (campaign_fullname, flight_id))
                continue

            impressions, clicks, spent = _get_usage(detail)

            # if the price changes then there may be multiple records for each campaign/date.
            values = campaign_details[(campaign, date)]
            values["impressions"] = values["impressions"] + impressions
            values["clicks"] = values["clicks"] + clicks
            values["spent_pennies"] = values["spent_pennies"] + (spent * 100.)

        for (campaign, date), values in campaign_details.iteritems():
            _insert_daily_campaign_reporting(
                codename=campaign._fullname,
                date=date,
                subreddit=campaign.target_name,
                **values
            )

    link.last_daily_report = report_id
    link.last_daily_report_run = queued_date
    link._commit()


def process_report_q():
    @g.stats.amqp_processor('adzerk_reporting_q')
    def _processor(message):
        data = json.loads(message.body)
        action = data.get("action")

        if action == "generate_daily_link_report":
            _handle_generate_daily_link_report(
                link_id=data.get("link_id"),
            )
        elif action == "generate_lifetime_campaign_report":
            _handle_generate_lifetime_campaign_report(
                campaign_id=data.get("campaign_id"),
            )
        else:
            g.log.warning("adzerk_reporting_q: unknown action - \"%s\"" % action)

    amqp.consume_items("adzerk_reporting_q", _processor, verbose=False)


def _insert_daily_link_reporting(
        codename, date, impressions,
        clicks, spent_pennies):

    date = date.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=None,
    )
    clicks_row = AdserverClickthroughsByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=clicks,
        pageview_count=clicks,
    )

    impressions_row = AdserverImpressionsByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=impressions,
        pageview_count=impressions,
    )

    spent_row = AdserverSpentPenniesByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=spent_pennies,
        pageview_count=spent_pennies,
    )

    Session.merge(clicks_row)
    Session.merge(impressions_row)
    Session.merge(spent_row)
    Session.commit()


def _insert_daily_campaign_reporting(
        codename, date, impressions,
        clicks, spent_pennies, subreddit=None):

    date = date.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
        tzinfo=None,
    )
    clicks_row = AdserverTargetedClickthroughsByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=clicks,
        pageview_count=clicks,
        subreddit=subreddit,
    )

    impressions_row = AdserverTargetedImpressionsByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=impressions,
        pageview_count=impressions,
        subreddit=subreddit,
    )

    spent_row = AdserverTargetedSpentPenniesByCodename(
        codename=codename,
        date=date,
        interval="day",
        unique_count=spent_pennies,
        pageview_count=spent_pennies,
        subreddit=subreddit,
    )

    Session.merge(clicks_row)
    Session.merge(impressions_row)
    Session.merge(spent_row)
    Session.commit()
