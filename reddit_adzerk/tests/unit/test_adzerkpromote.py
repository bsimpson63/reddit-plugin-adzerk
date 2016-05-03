from mock import MagicMock, Mock, patch
from random import randint

from r2.tests import RedditTestCase

from reddit_adzerk.adzerkpromote import flight_is_active


class TestIsActive(RedditTestCase):

    def test_flight_is_active(self):
        """
        Assert that `flight_is_active` returns `True` only if all kwargs are
        `False`.
        """
        kwarg_keys = (
            'needs_approval',
            'is_paused',
            'needs_payment',
            'is_terminated',
            'is_deleted',
            'is_overdelivered',
        )
        kwargs = dict()
        for key in kwarg_keys:
            kwargs[key] = False

        # Should return True only if all kwargs have value of False
        self.assertTrue(flight_is_active(**kwargs))

        # If any kwarg value is True, flight_is_active should return False
        random_kwarg_key = kwarg_keys[randint(0, len(kwarg_keys) - 1)]
        kwargs[random_kwarg_key] = True

        self.assertFalse(flight_is_active(**kwargs))

        # If all kwarg values are True, flight_is_active should return False
        for key in kwarg_keys:
            kwargs[key] = True

        self.assertFalse(flight_is_active(**kwargs))
