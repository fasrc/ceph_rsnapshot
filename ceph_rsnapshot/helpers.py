# helper functions


# allowed characters in settings strings here:
# alphanumeric, forward slash / and literal . and _ and -
# Note the - needs to be last in the re group
STRING_SAFE_CHAR_RE = "[a-zA-Z0-9/\._-]"


def validate_string(string):
    for char in string:
        if not re.search(STRING_SAFE_CHAR_RE, char):
            raise NameError('disallowed character (%s) in string: %s' % 
                            (char, string))
    return True


def get_current_settings():
    return(dict(
        CEPH_HOST=settings.CEPH_HOST,
        CEPH_USER=settings.CEPH_USER,
        CEPH_CLUSTER=settings.CEPH_CLUSTER,
        POOL=settings.POOL,
        QCOW_TEMP_PATH=settings.QCOW_TEMP_PATH,
        EXTRA_ARGS=settings.EXTRA_ARGS,
        TEMP_CONF_DIR_PREFIX=settings.TEMP_CONF_DIR_PREFIX,
        TEMP_CONF_DIR=settings.TEMP_CONF_DIR,
        BACKUP_BASE_PATH=settings.BACKUP_BASE_PATH,
        KEEPCONF=settings.KEEPCONF,
        LOG_BASE_PATH=settings.LOG_BASE_PATH,
        LOG_FILENAME=settings.LOG_FILENAME,
        VERBOSE=settings.VERBOSE,
        NOOP=settings.NOOP,
        NO_ROTATE_ORPHANS=settings.NO_ROTATE_ORPHANS,
        IMAGE_RE=settings.IMAGE_RE,
        RETAIN_INTERVAL=settings.RETAIN_INTERVAL,
        RETAIN_NUMBER=settings.RETAIN_NUMBER,
        SNAP_NAMING_DATE_FORMAT=settings.SNAP_NAMING_DATE_FORMAT,
        MIN_FREESPACE=settings.MIN_FREESPACE,
        SH_LOGGING=settings.SH_LOGGING,
    ))


def validate_settings_strings():
    """ check all settings strings to make sure they are only safe chars
        if not fail run
    """
    # check strings are safe
    current_settings = get_current_settings()
    for key in current_settings:
        if key in ['IMAGE_RE', 'SNAP_NAMING_DATE_FORMAT']:
            # these are allowed to have weird characters
            continue
        value = current_settings[key]
        if type(value) in [bool, int]:
            # don't compare these to an RE
            continue
        try:
            if helpers.validate_string(value):
                continue
        except NameError as e:
            # bad character in a string, fail run
            raise NameError('disallowed character in setting: %s'
                ' error %s' % (key, e))
        except Exception as e:
            raise