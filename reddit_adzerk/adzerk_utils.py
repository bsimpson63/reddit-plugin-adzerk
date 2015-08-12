def _join_queries(operator, *args):
    delimiter = ' %s ' % operator
    items = args[0] if isinstance(args[0], list) else args
    return delimiter.join(items)


def get_version_query(version_range):
    if not version_range:
        return

    lower_major, lower_minor = [int(bound) for bound in version_range[0].split('.')]

    lower_template = '($device.osVersion.major = %d and $device.osVersion.minor >= %d)'
    upper_template = '($device.osVersion.major = %d and $device.osVersion.minor <= %d)'

    # if there is an upper bound
    if version_range[1]:
        upper_major, upper_minor = [int(bound) for bound in version_range[1].split('.')]

        lower_range = lower_template % (lower_major, lower_minor)
        upper_range = upper_template % (upper_major, upper_minor)

        major_template = '($device.osVersion.major >= %d and $device.osVersion.major <= %d)'

        # if the min and max are the same
        if version_range[0] == version_range[1]:
            range_query = ('($device.osVersion.major = %d and $device.osVersion.minor = %d)' %
                             (lower_major, lower_minor))

        # if the min and max are the same major version (i.e., 5.1 & 5.9)
        elif lower_major == upper_major:
            range_query = _join_queries('and', lower_range, upper_range)

        # if the min major and max major are within 1 of each other
        elif abs(lower_major - upper_major) <= 1:
            range_query = _join_queries('or', lower_range, upper_range)

        # if the min minor is 0
        elif lower_minor == 0:
            major_range = major_template % (lower_major, upper_major - 1)
            range_query = _join_queries('or', major_range, upper_range)

        # everything else
        else:
            major_range = major_template % (lower_major + 1, upper_major - 1)
            range_query = _join_queries('or', major_range, lower_range, upper_range)

    # if there is no upper bound
    else:
        major_template = '($device.osVersion.major >= %d)'

        # if the min minor is 0
        if lower_minor == 0:
            major_range = major_template % lower_major
            range_query = major_range

        # everything else
        else:
            major_range = major_template % (lower_major + 1)
            lower_range = lower_template % (lower_major, lower_minor)
            range_query = _join_queries('or', major_range, lower_range)

    return range_query


def get_mobile_targeting_query(os_str='',
                               lookup_str='',
                               mobile_os=[],
                               devices=None,
                               versions=None):
    from adzerk_utils import get_version_query

    if os_str in mobile_os:
        queries = []

        os_query = '$device.os = "%s"' % os_str
        queries.append(os_query)

        if devices and versions:
            device_queries = ['$device.%s like "%s"' % (lookup_str, device)
                              for device in devices]
            device_query = '(%s)' % _join_queries('or', device_queries)
            version_query = '(%s)' % get_version_query(versions)

            queries.append(device_query)
            queries.append(version_query)

        return '(%s)' % _join_queries('and', queries)

    return None
