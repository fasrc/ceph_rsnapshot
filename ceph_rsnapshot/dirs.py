from ceph_rsnapshot import settings, logs
import os
import sys
import tempfile
import sh


def check_set_dir_perms(directory, perms=0o700):
    logger = logs.get_logger()
    desired_mode = oct(perms)[-3:]
    if settings.NOOP:
      logger.info('NOOP: would have verified that permissions on %s are %s' %
        ( directory, desired_mode ))
    else:
      dir_stat = os.stat(directory)
      current_mode = oct(dir_stat.st_mode)[-3:]
      if current_mode != desired_mode:
        logger.warning('perms not correct on %s: currently %s should be %s,'
                       'fixing' % (directory, current_mode, desired_mode))
        os.chmod(directory, perms)
        logger.info('perms now correctly set to %s on %s' % (desired_mode,
                                                             directory))


def setup_backup_dirs(pool='', dirs=''):
    logger = logs.get_logger()
    if not pool:
        pool = settings.POOL
    if not dirs:
        dirs = [settings.BACKUP_BASE_PATH,
                "%s/%s" % (settings.BACKUP_BASE_PATH, pool),
                ]
    # TODO support multiple pools
    # for pool in settings.POOLS
    #   dirs.append(pool)
    for directory in dirs:
        logger.info('setting up backup dir: %s' % directory)
        setup_dir(directory)


def setup_log_dirs(pool=''):
    logger = logs.get_logger()
    if not pool:
        pool = settings.POOL
    dirs = [
        settings.LOG_BASE_PATH,
        "%s/rsnap" % settings.LOG_BASE_PATH,
        "%s/rsnap/%s" % (settings.LOG_BASE_PATH, pool),
    ]
    for directory in dirs:
        setup_dir(directory)


def setup_temp_conf_dir(pool=''):
    if not pool:
        pool = settings.POOL
    logger = logs.get_logger()
    if settings.TEMP_CONF_DIR:
        # if it's been set, use it
        if os.path.isdir(settings.TEMP_CONF_DIR):
            logger.info('using temp conf dir %s' % settings.TEMP_CONF_DIR)
        else:
            try:
                if settings.NOOP:
                    logger.info('NOOP: would have made temp dir %s' %
                                settings.TEMP_CONF_DIR)
                else:
                    os.makedirs(settings.TEMP_CONF_DIR)
                    logger.info('created temp dir at %s' %
                                settings.TEMP_CONF_DIR)
            # TODO only catch IOerror here
            except IOError as e:
                logger.error('Cannot create conf temp dir (or intermediate dirs) from' +
                             ' setting %s with error %s' % (settings.TEMP_CONF_DIR, e))
                sys.exit(1)
    else:
        # if not, make one
        try:
            if settings.NOOP:
                logger.info('NOOP: would have made temp dir with mkdtemp'
                    ' prefix: %s' % settings.TEMP_CONF_DIR_PREFIX)
                temp_conf_dir = '/tmp/ceph_rsnapshot_mkdtemp_noop_fake_path'
            else:
                temp_conf_dir = tempfile.mkdtemp(
                    prefix=settings.TEMP_CONF_DIR_PREFIX)
                logger.info('created temp conf dir: %s' % temp_conf_dir)
            # store this in global settings
            settings.TEMP_CONF_DIR = temp_conf_dir
        # TODO only catch io error here
        except IOError as e:
            logger.error('cannot create conf temp dir with error %s' % e)
            sys.exit(1)
    # now make for pool
    if settings.NOOP:
        logger.info('NOOP: would have made temp conf subdir for pool %s' % pool)
    else:
        logger.info('creating temp conf subdir for pool %s' % pool)
        os.mkdir("%s/%s" % (settings.TEMP_CONF_DIR, pool), 0700)
    return settings.TEMP_CONF_DIR

# make path to export qcows to
# FIXME do this for all pools from the main loop


def setup_qcow_temp_path(pool='',cephhost='',qcowtemppath='',noop=None):
    """ ssh to ceph node and check or make temp qcow export path
    """
    logger = logs.get_logger()
    if not qcowtemppath:
        qcowtemppath = settings.QCOW_TEMP_PATH
    if not pool:
        pool = settings.POOL
    if not cephhost:
        cephhost = settings.CEPH_HOST
    if not noop:
        noop = settings.NOOP
    temp_path = '%s/%s' % (qcowtemppath, pool)
    logger.info('making qcow temp export path %s on ceph host %s' % (temp_path,
        cephhost))
    LS_COMMAND = 'ls %s' % temp_path
    MKDIR_COMMAND = 'mkdir -p %s' % temp_path
    CHMOD_COMMAND = 'chmod 700 %s' % temp_path
    try:
        ls_result = sh.ssh(cephhost,LS_COMMAND)
    except sh.ReturnErrorCode as e:
        if e.errno == 2:
            # ls returns 2 for no such dir, this is OK, just make it
            try:
                if noop:
                    logger.info('NOOP: would have made qcow temp path %s' %
                                temp_path)
                else:
                    sh.ssh(cephhost,MKDIR_COMMAND)
                    sh.ssh(cephhost,CHMOD_COMMAND)
            except sh.ReturnErrorCode as e:
                logger.error('error making or chmodding qcow temp dir:')
                logger.exception(e.stderr)
                raise
            except Exception as e:
                logger.error('error making or chmodding qcow temp dir')
                logger.exception(e)
                raise
        else:
            logger.error('error checking temp qcow export directory')
            logger.exception(e.stderr)
            raise
    except Exception as e:
        logger.error('error checking temp qcow export directory')
        logger.exception(e)
        raise
    # we rsnap an individual qcow so we don't need to check it's emoty
    logger.info('using qcow temp path: %s' % temp_path)
    # now just to be safe verify perms on it are 700
    try:
        sh.ssh(cephhost,CHMOD_COMMAND)
    except sh.ReturnErrorCode as e:
        logger.error('error chmodding qcow temp dir:')
        logger.exception(e.stderr)
        raise
    except Exception as e:
        logger.error('error chmodding qcow temp dir')
        logger.exception(e)
        raise


# check that empty_source is empty
# this is used to rotate orphans
# TODO make this use an empty tempdir
def make_empty_source():
    # TODO make this use mkdtemp
    empty_source_path = "%s/empty_source/" % settings.QCOW_TEMP_PATH
    # get logger we setup earlier
    logger = logs.get_logger()
    try:
        dirlist = os.listdir(empty_source_path)
        if len(dirlist) != 0:
            raise NameError('ERROR: empty_source_path %s exists and is not'
                ' empty' % empty_source_path)
    except OSError as e:
        if e.errno == 2:
            # OSError 2 is No such file or directory, so make it
            if settings.NOOP:
                logger.info('NOOP: would have made temp empty source at %s' %
                    empty_source_path)
            else:
                logger.info('creating temp empty source path %s' %
                    empty_source_path)
                os.mkdir(empty_source_path, 0700)
                # TODO catch if error?
        else:
            raise


def setup_dir(directory):
    logger = logs.get_logger()
    # make the if it doesn't exist
    if not os.path.isdir(directory):
        # make dir and preceeding dirs if necessary
        if settings.NOOP:
            logger.info('NOOP: would have run makedirs on path %s' % directory)
        else:
            os.makedirs(directory, 0700)
    else:
        logger.info('directory %s already exists, so using it' % directory)
        # still need to check perms
        check_set_dir_perms(directory)



def setup_dir_per_pool(directory):
    logger = logs.get_logger()
    # for pool in settings.POOLS
    #   dirs.append(pool)
    #   setup_dir(dirs)
    pass


def remove_temp_conf_dir():
    logger = logs.get_logger()
    if not settings.KEEPCONF:
        logger.info("removing temp conf dir %s" % settings.TEMP_CONF_DIR)
        try:
            if settings.NOOP:
                logger.info('would have removed temp conf dirs %s/%s and %s' %
                            (settings.TEMP_CONF_DIR, settings.POOL, settings.TEMP_CONF_DIR))
            else:
                # TODO all pools
                os.rmdir("%s/%s" % (settings.TEMP_CONF_DIR, settings.POOL))
                os.rmdir(settings.TEMP_CONF_DIR)
        except (IOError, OSError) as e:
            logger.warning("unable to remove temp conf dir with error %s" % e)
