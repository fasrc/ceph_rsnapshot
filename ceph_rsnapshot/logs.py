# logging setup

import sh
import sys
import os
import logging
from ceph_rsnapshot import settings


log = logging.getLogger('ceph_rsnapshot')

# setup logging to file, and if stdout is True, log to stdout as well
# granularity needed for gathernames replying via ssh
def setup_logging(stdout=True):
    log_location = settings.LOG_BASE_PATH
    log_filename = settings.LOG_FILENAME
    sh_logging = settings.SH_LOGGING
    verbose = settings.VERBOSE

    logger = logging.getLogger('ceph_rsnapshot')
    # get logger for sh module so we can configure it as well
    sh_logger = logging.getLogger('sh.command')

    log_level = logging.INFO
    if verbose:
        log_level = logging.DEBUG

    logger.setLevel(log_level)
    if sh_logging:
        sh_logger.setLevel(log_level)

    # setup log format
    logFormatter = logging.Formatter(settings.LOG_FORMAT)

    # if set to log to stdout, setup console loggers
    if stdout:
        consoleHandler = logging.StreamHandler(sys.stdout)
        consoleHandler.setFormatter(logFormatter)
        logger.addHandler(consoleHandler)
        if sh_logging:
            sh_logger.addHandler(consoleHandler)

    # setup logging dirs
    if not os.path.isdir(log_location):
        if settings.NOOP:
            logger.info('NOOP: would have made log dir at %s' % log_location)
        else:
            os.makedirs(log_location)
    if not os.path.isdir("%s/rsnap" % log_location):
        if settings.NOOP:
            logger.info('NOOP: would have made log dir at %s/rsnap' %
                        log_location)
        else:
            os.makedirs("%s/rsnap" % log_location)
    log_file = "%s/%s" % (log_location, log_filename)
    # logging.basicConfig(filename=log_file, level=log_level)

    # set format on file loggers
    if settings.NOOP:
        logger.info('would have added file logging')
    else:
        fileHandler = logging.FileHandler(log_file)
        fileHandler.setFormatter(logFormatter)
        logger.addHandler(fileHandler)
        if sh_logging:
            sh_logger.addHandler(fileHandler)

    # return logger
    return(logger)


def get_logger():
    return logging.getLogger('ceph_rsnapshot')
