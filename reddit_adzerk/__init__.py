from r2.lib.plugin import Plugin
from r2.lib.js import Module


class Adzerk(Plugin):
    needs_static_build = True

    js = {
        'reddit': Module('reddit.js',
            'adzerk/adzerk.js',
        )
    }

    def load_controllers(self):
        # replace the standard Ads view with an Adzerk specific one.
        import r2.lib.pages.pages
        from adzerkads import Ads as AdzerkAds
        r2.lib.pages.pages.Ads = AdzerkAds
