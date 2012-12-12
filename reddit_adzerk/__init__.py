from r2.lib.plugin import Plugin
from r2.lib.configparse import ConfigValue
from r2.lib.js import Module

class Adzerk(Plugin):
    needs_static_build = True

    js = {
        'reddit': Module('reddit.js',
            'adzerk/adzerk.js',
        )
    }
