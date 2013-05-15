from urllib import quote

from pylons import c, g

from r2.lib.pages import Ads as BaseAds


class Ads(BaseAds):
    def __init__(self):
        BaseAds.__init__(self)
        adzerk_all_the_things = g.live_config.get("adzerk_all_the_things")
        adzerk_srs = g.live_config.get("adzerk_srs")
        in_adzerk_sr = adzerk_srs and c.site.name.lower() in adzerk_srs
        if adzerk_all_the_things or in_adzerk_sr:
            url_key = "adzerk_https_url" if c.secure else "adzerk_url"
            site_name = getattr(c.site, "analytics_name", c.site.name)
            self.ad_url = g.config[url_key].format(
                subreddit=quote(site_name.lower()),
                origin=c.request_origin,
            )
            self.frame_id = "ad_main"
