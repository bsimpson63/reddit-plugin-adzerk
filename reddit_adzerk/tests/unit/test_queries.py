from unittest import TestCase

from reddit_adzerk.adzerk_utils import (get_mobile_targeting_query,
                                        get_version_query)


class ZerkelRangeQueryTest(TestCase):

    def test_with_upper_1(self):
        """Assert output when there is an upper limit"""
        input = ['1.1', '3.4']
        expected = ('($device.osVersion.major >= 2 AND ' +
                    '$device.osVersion.major <= 2) ' +
                    'OR ' +
                    '($device.osVersion.major = 1 AND ' +
                    '$device.osVersion.minor >= 1) ' +
                    'OR ' +
                    '($device.osVersion.major = 3 AND ' +
                    '$device.osVersion.minor <= 4)')
        output = get_version_query(input)
        self.assertEqual(output, expected)

    def test_with_upper_2(self):
        """Assert output when there is an upper limit"""
        input = ['2.4', '5.1']
        expected = ('($device.osVersion.major >= 3 AND ' +
                    '$device.osVersion.major <= 4) ' +
                    'OR ' +
                    '($device.osVersion.major = 2 AND ' +
                    '$device.osVersion.minor >= 4) ' +
                    'OR ' +
                    '($device.osVersion.major = 5 AND ' +
                    '$device.osVersion.minor <= 1)')
        output = get_version_query(input)
        self.assertEqual(output, expected)

    def test_within_one(self):
        """Assert output when lower and upper are within one of each other"""
        input = ['5.2', '6.1']
        expected = ('($device.osVersion.major = 5 AND ' +
                    '$device.osVersion.minor >= 2) ' +
                    'OR ' +
                    '($device.osVersion.major = 6 AND ' +
                    '$device.osVersion.minor <= 1)')
        output = get_version_query(input)
        self.assertEqual(output, expected)

    def test_same(self):
        """Assert output when lower and upper are identical"""
        input = ['1.1', '1.1']
        expected = ('($device.osVersion.major = 1 AND ' +
                    '$device.osVersion.minor = 1)')
        output = get_version_query(input)
        self.assertEqual(output, expected)

    def test_same_major(self):
        """Assert output when lower and upper share the same major"""
        input = ['5.5', '5.6']
        expected = ('($device.osVersion.major = 5 AND ' +
                    '$device.osVersion.minor >= 5) ' +
                    'AND ' +
                    '($device.osVersion.major = 5 AND ' +
                    '$device.osVersion.minor <= 6)')
        output = get_version_query(input)
        self.assertEqual(output, expected)

    def test_min_minor_zero(self):
        """Assert output when lower has minor == 0"""
        input = ['2.0', '6.0']
        expected = ('($device.osVersion.major >= 2 AND ' +
                    '$device.osVersion.major <= 5) ' +
                    'OR ' +
                    '($device.osVersion.major = 6 AND ' +
                    '$device.osVersion.minor <= 0)')
        output = get_version_query(input)
        self.assertEqual(output, expected)

    def test_no_upper(self):
        """Assert output when there is no upper limit"""
        input = ['3.3', '']
        expected = ('($device.osVersion.major >= 4) ' +
                    'OR ' +
                    '($device.osVersion.major = 3 AND ' +
                    '$device.osVersion.minor >= 3)')
        output = get_version_query(input)
        self.assertEqual(output, expected)

    def test_no_upper_min_minor_zero(self):
        """Assert output when there is no upper limit and lower minor == 0"""
        input = ['3.0', '']
        expected = '($device.osVersion.major >= 3)'
        output = get_version_query(input)
        self.assertEqual(output, expected)


class ZerkelMobileTargetingQueryTest(TestCase):

    def test_ios_detailed_targeting(self):
        """Assert output when targeting iOS device and version"""
        os_str = 'iOS'
        lookup_str = 'modelName'
        devices = ['iPhone', 'iPad']
        versions = ['1.1', '']
        expected = ('($device.os = "iOS" AND ' +
                    '($device.modelName CONTAINS "iPhone" OR ' +
                    '$device.modelName CONTAINS "iPad") AND ' +
                    '(($device.osVersion.major >= 2) OR ' +
                    '($device.osVersion.major = 1 AND ' +
                    '$device.osVersion.minor >= 1)))')
        output = get_mobile_targeting_query(os_str, lookup_str, devices,
                                            versions)
        self.assertEqual(output, expected)

    def test_android_detailed_targeting(self):
        """Assert output when targeting Android device and version"""
        os_str = 'Android'
        lookup_str = 'formFactor'
        devices = ['tablet']
        versions = ['4.4', '4.4']
        expected = ('($device.os = "Android" AND ' +
                    '($device.formFactor CONTAINS "tablet") AND ' +
                    '(($device.osVersion.major = 4 AND ' +
                    '$device.osVersion.minor = 4)))')
        output = get_mobile_targeting_query(os_str, lookup_str, devices,
                                            versions)
        self.assertEqual(output, expected)

    def test_ios_generic_targeting(self):
        """Assert output when targeting all iOS"""
        os_str = 'iOS'
        expected = '($device.os = "iOS")'
        output = get_mobile_targeting_query(os_str)
        self.assertEqual(output, expected)

    def test_android_generic_targeting(self):
        """Assert output when targeting all Android"""
        os_str = 'Android'
        expected = '($device.os = "Android")'
        output = get_mobile_targeting_query(os_str)
        self.assertEqual(output, expected)
