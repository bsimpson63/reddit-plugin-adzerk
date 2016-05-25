import json
import pkg_resources

from pylons import app_globals as g
from pylons.i18n import N_

from r2.lib.plugin import Plugin
from r2.lib.configparse import ConfigValue
from r2.lib.js import Module


class Adzerk(Plugin):
    needs_static_build = True

    errors = {
        "INVALID_SITE_PATH":
            N_("invalid site path/name"),
    }

    config = {
        ConfigValue.str: [
            'adzerk_engine_domain',
        ],

        ConfigValue.int: [
            'az_selfserve_salesperson_id',
            'az_selfserve_network_id',
            'az_reporting_timeout',
        ],

        ConfigValue.float: [
            'display_ad_skip_probability',
        ],

        ConfigValue.tuple: [
            'display_ad_skip_keywords',
        ],

        ConfigValue.dict(ConfigValue.str, ConfigValue.int): [
            'az_selfserve_priorities',
            'az_selfserve_site_ids',
        ],

        ConfigValue.tuple_of(ConfigValue.int): [
            'adserver_campaign_ids',
        ],
    }

    live_config = {

        ConfigValue.float: [
            'events_collector_ad_serving_sample_rate',
            'ad_log_sample_rate',
        ],

        ConfigValue.int: [
            'adx_passback_id',
        ],

    }

    js = {
        'reddit-init': Module('reddit-init.js',
            'adzerk/adzerk.js',
        ),

        'display': Module('display.js',
            'lib/json2.js',
            'custom-event.js',
            'frames.js',
            'adzerk/display.js',
        ),

        'companion': Module('companion.js',
            'adzerk/companion.js',
        ),

        'ad-dependencies': Module('ad-dependencies.js',
            'adzerk/jquery.js',
        ),
    }

    def add_routes(self, mc):
        mc('/api/request_promo/', controller='adzerkapi', action='request_promo')
        mc('/ads/display/300x250/', controller='adserving', action='ad_300_250')
        mc('/ads/display/300x250-companion/', controller='adserving', action='ad_300_250_companion')
        mc('/ads/adx-passback', controller='adx', action='passback')

    def declare_queues(self, queues):
        from r2.config.queues import MessageQueue
        queues.declare({
            "adzerk_q": MessageQueue(bind_to_self=True),
            "adzerk_reporting_q": MessageQueue(bind_to_self=True),
        })

    def load_controllers(self):
        # replace the standard Ads view with an Adzerk specific one.
        import r2.lib.pages.pages
        from adzerkads import Ads as AdzerkAds
        from lib.events import AdEventQueue

        r2.lib.pages.pages.Ads = AdzerkAds

        g.ad_events = AdEventQueue()

        # replace standard adserver with Adzerk.
        from adzerkpromote import AdzerkApiController
        from adzerkpromote import hooks as adzerkpromote_hooks
        from adzerkads import AdServingController
        from adzerkads import AdXController
        adzerkpromote_hooks.register_all()
