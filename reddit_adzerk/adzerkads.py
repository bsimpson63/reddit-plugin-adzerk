import json
import random
from urllib import quote

from pylons import tmpl_context as c
from pylons import app_globals as g
from pylons import request

from r2.controllers import add_controller
from r2.controllers.reddit_base import (
    MinimalController,
    UnloggedUser,
)
from r2.lib import promote
from r2.lib.pages import Ads as BaseAds
from r2.lib.wrapped import Templated
from r2.models import Subreddit

class Ads(BaseAds):
    def __init__(self):
        BaseAds.__init__(self)

        keywords = promote.keywords_from_context(
            c.user, c.site,
            include_subscriptions=False,
        )

        data = {
            "keywords": list(keywords),
            "origin": c.request_origin,
        }

        if isinstance(c.site, Subreddit) and not c.default_sr:
            data["subreddit"] = c.site.name

        placements = request.GET.get("placements", None)

        if c.user_is_sponsor and placements:
            data["placements"] = placements

        self.ad_url = g.adzerk_url.format(data=json.dumps(data))
        self.frame_id = "ad_main"


class BaseAdFrame(Templated):
    pass


class Ad300x250(BaseAdFrame):
    pass


class Ad300x250Companion(BaseAdFrame):
    pass


class Passback(Templated):
    pass


@add_controller
class AdServingController(MinimalController):
    def pre(self):
        super(AdServingController, self).pre()

        if request.host != g.media_domain:
            # don't serve up untrusted content except on our
            # specifically untrusted domain
            self.abort404()

        c.user = UnloggedUser([c.lang])
        c.user_is_loggedin = False
        c.forced_loggedout = True
        c.allow_framing = True

    def GET_ad_300_250(self):
        return Ad300x250().render()

    def GET_ad_300_250_companion(self):
        return Ad300x250Companion().render()

@add_controller
class AdXController(MinimalController):

    def GET_passback(self):
        return Passback(passback_id=g.live_config["adx_passback_id"]).render()

