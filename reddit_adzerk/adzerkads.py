from urllib import quote

from pylons import c, g

from r2.lib.pages import Ads as BaseAds


class Ads(BaseAds):
    def __init__(self):
        BaseAds.__init__(self)
        adzerk_test_srs = g.live_config.get("adzerk_test_srs")
        if adzerk_test_srs and c.site.name.lower() in adzerk_test_srs:
            url_key = "adzerk_https_url" if c.secure else "adzerk_url"
            self.ad_url = g.config[url_key].format(
                subreddit=quote(c.site.name),
                origin=c.request_origin,
            )
            self.frame_id = "ad_main"
