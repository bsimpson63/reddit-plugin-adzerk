from urllib import quote

from pylons import c, g

from r2.lib.pages import Ads as BaseAds


class Ads(BaseAds):
    def __init__(self):
        BaseAds.__init__(self)

        url_key = "adzerk_https_url" if c.secure else "adzerk_url"
        site_name = getattr(c.site, "analytics_name", c.site.name)
        self.ad_url = g.config[url_key].format(
            subreddit=quote(site_name.lower()),
            origin=c.request_origin,
        )
        self.frame_id = "ad_main"
