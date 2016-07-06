from baseplate.events import FieldKind
from pylons import app_globals as g

from r2.lib.eventcollector import (
    EventQueue,
    Event,
    squelch_exceptions,
)
from r2.lib.utils import sampled
from r2.models import (
    FakeSubreddit,
)


class AdEvent(Event):
    @classmethod
    def get_context_data(cls, request, context):
        data = super(AdEvent, cls).get_context_data(request, context)
        dnt_header = request.headers.get("DNT", None)

        if dnt_header is not None:
            data["dnt"] = dnt_header == "1"

        return data


class AdzerkAPIEvent(Event):
    def add_target_fields(self, thing):
        self.add("target_fullname", thing._fullname)
        self.add("target_type", thing.__class__.__name__)
        self.add("is_deleted", thing._deleted)

    def add_caller_fields(self, user):
        if user:
            self.add("caller_user_id", user._id)
            self.add("caller_user_name", user.name)
        else:
            self.add("is_automated", True)

    def add_error_fields(self, error):
        if error:
            self.add("error_status_code", error.status_code)
            self.add("error_body", error.response_body)


class AdEventQueue(EventQueue):
    @squelch_exceptions
    @sampled("events_collector_ad_serving_sample_rate")
    def ad_request(
            self,
            keywords,
            platform,
            placement_name,
            placement_types,
            is_refresh,
            subreddit=None,
            request=None,
            context=None,
        ):
        """Create an `ad_request` for event-collector.

        keywords: Array of keywords used to select the ad.
        platform: The platform the ad was requested for.
        placement_name: The identifier of the placement.
        placement_types: Array of placements types.
        is_refresh: Whether or not the request is for the initial ad or a
            refresh after refocusing the page.
        subreddit: The Subreddit of the ad was  displayed on.
        request, context: Should be pylons.request & pylons.c respectively;

        """
        event = AdEvent(
            topic="ad_serving_events",
            event_type="ss.ad_request",
            request=request,
            context=context,
        )

        event.add("keywords", keywords)
        event.add("platform", platform)
        event.add("placement_name", placement_name)
        event.add("placement_types", placement_types)
        event.add("is_refresh", is_refresh)

        if not isinstance(subreddit, FakeSubreddit):
            event.add_subreddit_fields(subreddit)

        self.save_event(event)

    @squelch_exceptions
    @sampled("events_collector_ad_serving_sample_rate")
    def ad_response(
            self,
            keywords,
            platform,
            placement_name,
            placement_types,
            ad_id,
            impression_id,
            matched_keywords,
            rate_type,
            clearing_price,
            link_fullname=None,
            campaign_fullname=None,
            subreddit=None,
            priority=None,
            ecpm=None,
            request=None,
            context=None,
        ):
        """Create an `ad_response` for event-collector.

        keywords: Array of keywords used to select the ad.
        platform: The platform the ad was requested for.
        placement_name: The identifier of the placement.
        placement_types: Array of placements types.
        ad_id: Unique id of the ad response.
        impression_id: Unique id of the impression.
        matched_keywords: An array of the keywords which matched for the ad.
        rate_type: Flat/CPM/CPC/etc.
        clearing_price: What was paid for the rate type.
        link_fullname: The fullname of the promoted link.
        campaign_fullname: The fullname of the PromoCampaign.
        subreddit: The Subreddit of the ad was  displayed on.
        priority: The priority name of the ad.
        ecpm: The effective cpm of the ad.
        request, context: Should be pylons.request & pylons.c respectively;

        """
        event = AdEvent(
            topic="ad_serving_events",
            event_type="ss.ad_response",
            request=request,
            context=context,
        )

        event.add("platform", platform)
        event.add("placement_name", placement_name)
        event.add("placement_types", placement_types)
        event.add("ad_id", ad_id)
        event.add("impression_id",
                  impression_id, kind=FieldKind.HIGH_CARDINALITY)
        event.add("rate_type", rate_type)
        event.add("clearing_price", clearing_price)
        event.add("link_fullname", link_fullname)
        event.add("campaign_fullname", campaign_fullname)
        event.add("priority", priority)
        event.add("ecpm", ecpm)

        # keywords are case insensitive, normalize and sort them
        # for easier equality testing.
        keywords = sorted(k.lower() for k in keywords)
        matched_keywords = sorted(k.lower() for k in matched_keywords)

        event.add("keywords", keywords)
        event.add("matched_keywords", matched_keywords)

        if not isinstance(subreddit, FakeSubreddit):
            event.add_subreddit_fields(subreddit)

        self.save_event(event)

    @squelch_exceptions
    def adzerk_api_request(
            self,
            request_type,
            thing,
            request_body,
            triggered_by=None,
            additional_data=None,
            request_error=None,
        ):
        """
        Create an `adzerk_api_events` event for event-collector.

        request_type: The type of request being made
        thing: The `Thing` which the request data is derived from
        request_body: The JSON payload to be sent to adzerk
        triggered_by: The user who triggered the API call
        additional_data: A dict of any additional meta data that may be
            relevant to the request
        request_error: An `adzerk_api.AdzerkError` if the request fails

        """
        event = AdzerkAPIEvent(
            topic='adzerk_api_events',
            event_type='ss.%s_request' % request_type,
        )

        event.add_target_fields(thing)
        event.add_caller_fields(triggered_by)
        event.add_error_fields(request_error)
        event.add("request_body", request_body)

        if additional_data:
            for key, value in additional_data.iteritems():
                event.add(key, value)

        self.save_event(event)
