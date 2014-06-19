from urllib import quote

from pylons import c, g

from r2.lib.pages import Ads as BaseAds
from r2.models.subreddit import DefaultSR


FRONTPAGE_NAME = "-reddit.com"

class Ads(BaseAds):
    def __init__(self):
        BaseAds.__init__(self)

        url_key = "adzerk_https_url" if c.secure else "adzerk_url"
        site_name = getattr(c.site, "analytics_name", c.site.name)

        # adzerk reporting is easier when not using a space in the tag
        if isinstance(c.site, DefaultSR):
            site_name = FRONTPAGE_NAME

        self.ad_url = g.config[url_key].format(
            subreddit=quote(site_name.lower()),
            origin=c.request_origin,
            loggedin="loggedin" if c.user_is_loggedin else "loggedout",
        )
        self.frame_id = "ad_main"
