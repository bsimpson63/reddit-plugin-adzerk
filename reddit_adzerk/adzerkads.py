from pylons import c, g

from r2.lib.pages import Ads as BaseAds


class Ads(BaseAds):
    def __init__(self):
        BaseAds.__init__(self)
        adzerk_test_srs = g.live_config.get("adzerk_test_srs")
        if adzerk_test_srs and c.site.name in adzerk_test_srs:
            if c.secure:
                self.ad_url = g.config["adzerk_https_url"].format(
                                origin=g.https_endpoint)
            else:
                self.ad_url = g.config["adzerk_url"].format(
                                origin=g.origin)
            self.frame_id = "ad_main"
