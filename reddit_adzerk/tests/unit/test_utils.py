from mock import MagicMock, Mock, patch
from unittest import TestCase

from reddit_adzerk.adzerk_utils import (_join_queries,
                                        get_version_query,
                                        get_mobile_targeting_query)

class ZerkelUtilsTest(TestCase):

    def test_join_queries_upper(self):
        """Return query string with uppercased operator"""
        operator = 'foo'
        args = ['bar', 'baz']
        returned_value = _join_queries(operator, args)

        # Operator should be uppercased
        self.assertTrue(operator.upper() in returned_value)

    def test_join_queries_construction(self):
        """Return correct number of operators"""
        operator = 'foo'
        args = ['bar']
        returned_value = _join_queries(operator, args)

        # With only one arg, operator should not appear
        self.assertFalse(operator in returned_value)
        # The return value should be the lone arg
        self.assertEquals(returned_value, args[0])

        args.extend(('baz', 'qux'))
        returned_value = _join_queries('foo', args)

        # Operator should still not appear
        self.assertFalse(operator in returned_value)
        # Operator should appear one less time than length of args
        self.assertEquals(returned_value.count(operator.upper()),
                          len(args) - 1)

    def test_join_queries_args(self):
        """Throw exception if args is not list or splat of strings"""
        operator = 'foo'
        args = ('bar', 'baz')

        # Args should not a tuple
        with self.assertRaises(TypeError) as error:
            _join_queries(operator, args)
        self.assertEquals(type(error.exception), TypeError)

        # Args should not a sequence of integers or floats
        with self.assertRaises(TypeError) as error:
            _join_queries(operator, 1, 1.1)
        self.assertEquals(type(error.exception), TypeError)

        # Args can be a list of strings
        args = ['bar', 'baz']
        returned_value = _join_queries(operator, args)
        self.assertEquals(type(returned_value), str)

        # Args can be a sequence of strings
        returned_value = _join_queries(operator, 'bar', 'baz', 'qux')
        self.assertEquals(type(returned_value), str)

    @patch('reddit_adzerk.adzerk_utils.get_version_query')
    def test_targeting_query(self, version_query):
        """Call get_version_query if devices and versions are passed"""
        get_mobile_targeting_query()
        self.assertFalse(version_query.called)

        get_mobile_targeting_query(devices=MagicMock(), versions=MagicMock())
        self.assertTrue(version_query.called)
