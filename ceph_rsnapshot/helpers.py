# helper functions

import re

from ceph_rsnapshot import settings
from ceph_rsnapshot import logs



def validate_string(string,additional_safe_chars=''):
    if additional_safe_chars:
        allowed_chars = re.sub('-]','%s-]' % additional_safe_chars,
            settings.STRING_SAFE_CHAR_RE)
    else:
        allowed_chars = settings.STRING_SAFE_CHAR_RE
    for char in string:
        if not re.search(allowed_chars, char):
            raise NameError('disallowed character (%s) in string: %s' % 
                            (char, string))
    return True


def get_current_settings():
    return(dict(
        CEPH_HOST=settings.CEPH_HOST,
        CEPH_USER=settings.CEPH_USER,
        CEPH_CLUSTER=settings.CEPH_CLUSTER,
        POOLS=settings.POOLS,
        POOL=settings.POOL,
        QCOW_TEMP_PATH=settings.QCOW_TEMP_PATH,
        TEMP_CONF_DIR_PREFIX=settings.TEMP_CONF_DIR_PREFIX,
        TEMP_CONF_DIR=settings.TEMP_CONF_DIR,
        KEEPCONF=settings.KEEPCONF,
        BACKUP_BASE_PATH=settings.BACKUP_BASE_PATH,
        LOG_BASE_PATH=settings.LOG_BASE_PATH,
        LOG_FILENAME=settings.LOG_FILENAME,
        STATUS_FILENAME=settings.STATUS_FILENAME,
        VERBOSE=settings.VERBOSE,
        NOOP=settings.NOOP,
        NO_ROTATE_ORPHANS=settings.NO_ROTATE_ORPHANS,
        IMAGE_RE=settings.IMAGE_RE,
        RETAIN_INTERVAL=settings.RETAIN_INTERVAL,
        RETAIN_NUMBER=settings.RETAIN_NUMBER,
        EXTRA_ARGS=settings.EXTRA_ARGS,
        SNAP_NAMING_DATE_FORMAT=settings.SNAP_NAMING_DATE_FORMAT,
        SNAP_DATE=settings.SNAP_DATE,
        USE_SNAP_STATUS_FILE=settings.USE_SNAP_STATUS_FILE,
        SNAP_STATUS_FILE_PATH=settings.SNAP_STATUS_FILE_PATH,
        MIN_FREESPACE=settings.MIN_FREESPACE,
        SH_LOGGING=settings.SH_LOGGING,
    ))


def validate_settings_strings():
    """ check all settings strings to make sure they are only safe chars
        if not fail run
    """
    logger = logs.get_logger()
    logger.info('checking settings strings to ensure they only contain safe'
        ' chars: %s' % settings.STRING_SAFE_CHAR_RE)
    # check strings are safe
    current_settings = get_current_settings()
    for key in current_settings:
        if key in settings.ADDITIONAL_SAFE_CHARS:
            additional_safe_chars = settings.ADDITIONAL_SAFE_CHARS[key]
        else:
            additional_safe_chars=''
        if key in ['IMAGE_RE']:
            # these are allowed to have weird characters
            # also this is only used in this script as a RE
            continue
        value = current_settings[key]
        if type(value) in [bool, int]:
            # don't compare these to an RE
            continue
        try:
            if key == 'POOLS':
                logger.info('checking pools string %s' % value)
                pools_arr = value.split(',')
                for pool in pools_arr:
                    logger.info('checking string for pool %s' % pool)
                    validate_string(pool,
                        additional_safe_chars=additional_safe_chars)
                # if here then they all validated
                continue
            if validate_string(value,
                               additional_safe_chars=additional_safe_chars):
                continue
        except NameError as e:
            # bad character in a string, fail run
            raise NameError('disallowed character in setting: %s'
                ' error %s' % (key, e))
        except Exception as e:
            raise
    logger.info('all settings strings ok')
