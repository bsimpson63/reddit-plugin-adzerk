import re

from r2.lib.errors import errors
from r2.lib.validator import (
    VMultiByPath,
    Validator,
)
from r2.models import (
    NotFound,
    Subreddit,
    MultiReddit,
)

is_multi_rx = re.compile(r"\A/?(user|r)/[^\/]+/m/(?P<name>.*?)/?\Z")
is_adhoc_multi_rx = re.compile(r"\A\/r\/((?:[0-z]+\+)+(?:[0-z])+)\Z")

class VSite(Validator):
    def __init__(self, param, required=True, *args, **kwargs):
        super(VSite, self).__init__(param, *args, **kwargs)
        self.required = required

    def run(self, path):
        if not self.required and not path:
            return
        adhoc_multi_rx = is_adhoc_multi_rx.match(path)
        if is_multi_rx.match(path):
            return VMultiByPath(self.param, kinds=("m")).run(path)
        elif adhoc_multi_rx:
            sr_strings = adhoc_multi_rx.groups()[0].split("+")
            srs = Subreddit._by_name(sr_strings, stale=True).values()
            return MultiReddit(path, srs)
        else:
            try:
                return Subreddit._by_name(path)
            except NotFound:
                self.set_error(errors.INVALID_SITE_PATH)
