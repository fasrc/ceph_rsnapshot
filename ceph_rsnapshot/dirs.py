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
                    (directory, desired_mode))
    else:
        dir_stat = os.stat(directory)
        current_mode = oct(dir_stat.st_mode)[-3:]
        if current_mode != desired_mode:
            logger.warning('perms not correct on %s: currently %s should be %s,'
                           'fixing' % (directory, current_mode, desired_mode))
            os.chmod(directory, perms)
            logger.info('perms now correctly set to %s on %s' % (desired_mode,
                                                                 directory))


def setup_dir(directory):
    logger = logs.get_logger()
    if not os.path.isdir(directory):
        # make dir and preceeding dirs if necessary
        if settings.NOOP:
            logger.info('NOOP: would have run makedirs on path %s' % directory)
        else:
            logger.info('creating directory 0700 %s' % directory)
            os.makedirs(directory, 0700)
    else:
        logger.info('directory %s already exists, so using it' % directory)
        # still need to check perms
        check_set_dir_perms(directory)


def setup_backup_dirs_for_pool(pool='', dirs=''):
    """ wrapper of the above to setup backup dirs
    """
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


def setup_log_dirs_for_pool(pool=''):
    """ wrapper of the above to setup log dirs
    """
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


def setup_temp_conf_dir_for_pool(pool=''):
    """ setup temp conf dir for rsnap confs
        if provided, use, otherwise make temp dir
    """
    if not pool:
        pool = settings.POOL
    logger = logs.get_logger()
    if settings.TEMP_CONF_DIR:
        if os.path.isdir(settings.TEMP_CONF_DIR):
            logger.info('using temp conf dir %s' % settings.TEMP_CONF_DIR)
        else:
            try:
                setup_dir(settings.TEMP_CONF_DIR)
            except IOError as e:
                logger.error('Cannot create conf temp dir (or intermediate dirs) from' +
                             ' setting %s with error %s' % (settings.TEMP_CONF_DIR, e))
                raise
    else:
        try:
            temp_conf_dir = make_empty_tempdir(
                prefix=settings.TEMP_CONF_DIR_PREFIX)
            # store this in global settings
            settings.TEMP_CONF_DIR = temp_conf_dir
        except IOError as e:
            logger.error('cannot create conf temp dir with error %s' % e)
            raise
    # now make for pool
    pool_conf_dir = "%s/%s" % (settings.TEMP_CONF_DIR, pool)
    setup_dir(pool_conf_dir)
    return settings.TEMP_CONF_DIR


def setup_qcow_temp_path(pool='', cephhost='', qcowtemppath='', noop=None):
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
    CHMOD_COMMAND = 'LANG='' LC_CTYPE='' chmod 700 %s' % temp_path
    try:
        ls_result = sh.ssh(cephhost, LS_COMMAND)
    except sh.ErrorReturnCode as e:
        if e.exit_code == 2:
            # ls returns 2 for no such dir, this is OK, just make it
            try:
                if noop:
                    logger.info('NOOP: would have made qcow temp path %s' %
                                temp_path)
                else:
                    sh.ssh(cephhost, MKDIR_COMMAND)
                    sh.ssh(cephhost, CHMOD_COMMAND)
            except sh.ErrorReturnCode as e:
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
        if settings.NOOP:
            logger.info('NOOP: would have chmodded qcow temp path with command:'
                ' %s' % CHMOD_COMMAND)
        else:
            sh.ssh(cephhost, CHMOD_COMMAND)
    except sh.ErrorReturnCode as e:
        logger.error('error chmodding qcow temp dir:')
        logger.exception(e.stderr)
        raise
    except Exception as e:
        logger.error('error chmodding qcow temp dir')
        logger.exception(e)
        raise


def check_empty_dir(directory):
    """ check a dir is empty
    """
    if os.path.isdir(directory):
        dirlist = os.listdir(path)
        if len(dirlist) != 0:
            raise NameError('ERROR: directory %s is not empty' % directory)
    else:
        raise NameError('directory %s does not exist' % directory)


def make_empty_tempdir(prefix=''):
    """ make an empty tempdir
    """
    logger = logs.get_logger()
    if not prefix:
        prefix = 'empty_'
    if settings.NOOP:
        logger.info('would have made an empty tempdir')
        return 'noop_fake_empty_path_with_prefix_%s' % prefix
    else:
        logger.info('creating tempdir with prefix %s' % prefix)
        empty_tempdir = tempfile.mkdtemp(prefix=prefix)
        return empty_tempdir


def remove_empty_dir(directory):
    """ remove a directory if it's empty
    """
    logger = logs.get_logger()
    if settings.NOOP:
        logger.info('would have removed %s' % directory)
    else:
        logger.info('removing %s' % directory)
        os.rmdir(directory)


def remove_temp_conf_dir():
    """ remove temp conf dirs
    """
    logger = logs.get_logger()
    if not settings.KEEPCONF:
        logger.info("removing temp conf dir %s" % settings.TEMP_CONF_DIR)
        try:
            if settings.NOOP:
                logger.info('would have removed temp conf dirs %s/%s and %s' %
                            (settings.TEMP_CONF_DIR, settings.POOL,
                             settings.TEMP_CONF_DIR))
            else:
                # TODO all pools
                os.rmdir("%s/%s" % (settings.TEMP_CONF_DIR, settings.POOL))
                os.rmdir(settings.TEMP_CONF_DIR)
        except (IOError, OSError) as e:
            logger.warning("unable to remove temp conf dir with error %s" % e)
