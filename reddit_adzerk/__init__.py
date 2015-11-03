import json
import pkg_resources

from r2.lib.plugin import Plugin
from r2.lib.configparse import ConfigValue
from r2.lib.js import Module


class Adzerk(Plugin):
    needs_static_build = True

    config = {
        ConfigValue.str: [
            'adzerk_engine_domain',
        ],

        ConfigValue.int: [
            'az_selfserve_site_id',
            'az_selfserve_salesperson_id',
            'az_selfserve_mobile_web_site_id',
            'az_selfserve_network_id',
        ],

        ConfigValue.dict(ConfigValue.str, ConfigValue.int): [
            'az_selfserve_priorities',
        ],

        ConfigValue.tuple_of(ConfigValue.int): [
            'adserver_campaign_ids',
        ],
    }

    js = {
        'reddit-init': Module('reddit-init.js',
            'adzerk/adzerk.js',
        ),

        'display': Module('display.js',
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
        r2.lib.pages.pages.Ads = AdzerkAds

        # replace standard adserver with Adzerk.
        from adzerkpromote import AdzerkApiController
        from adzerkpromote import hooks as adzerkpromote_hooks
        from adzerkads import AdServingController
        adzerkpromote_hooks.register_all()
