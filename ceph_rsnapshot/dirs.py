from ceph_rsnapshot import settings, logs
import os, sys, tempfile


def setup_backup_dirs(pool='',dirs=''):
  logger = logs.get_logger()
  if not pool:
    pool=settings.POOL
  if not dirs:
    dirs = [ settings.BACKUP_BASE_PATH,
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
    pool=settings.POOL
  dirs = [
      settings.LOG_BASE_PATH,
      "%s/rsnap" % settings.LOG_BASE_PATH,
      "%s/rsnap/%s" % (settings.LOG_BASE_PATH, pool),
    ]
  for directory in dirs:
    setup_dir(directory)

def setup_temp_conf_dir(pool=''):
  if not pool:
    pool=settings.POOL
  logger = logs.get_logger()
  if settings.TEMP_CONF_DIR:
    # if it's been set, use it
    if os.path.isdir(settings.TEMP_CONF_DIR):
      logger.info('using temp conf dir %s' % settings.TEMP_CONF_DIR)
    else:
      try:
        if settings.NOOP:
          logger.info('NOOP: would have made temp dir %s' % settings.TEMP_CONF_DIR)
        else:
          os.makedirs(settings.TEMP_CONF_DIR)
          logger.info('created temp dir at %s' % settings.TEMP_CONF_DIR)
      except Exception as e:
        logger.error('Cannot create conf temp dir (or intermediate dirs) from'+
        ' setting %s with error %s' % (settings.TEMP_CONF_DIR, e))
        sys.exit(1)
  else:
    # if not, make one
    try:
      if settings.NOOP:
        logger.info('NOOP: would have made temp dir with mkdtemp prefix' % settings.TEMP_CONF_DIR_PREFIX)
        temp_conf_dir = '/tmp/ceph_rsnapshot_mkdtemp_noop_fake_path'
      else:
        temp_conf_dir = tempfile.mkdtemp(prefix=settings.TEMP_CONF_DIR_PREFIX)
        logger.info('created temp conf dir: %s' % temp_conf_dir)
      # store this in global settings
      settings.TEMP_CONF_DIR = temp_conf_dir
    # TODO only catch io error here
    except Exception as e:
      logger.error('cannot create conf temp dir with error %s' % e)
      sys.exit(1)
  # now make for pool
  if settings.NOOP:
    logger.info('NOOP: would have made temp conf subdir for pool %s' % pool)
  else:
    logger.info('creating temp conf subdir for pool %s' % pool)
    os.mkdir("%s/%s" % (settings.TEMP_CONF_DIR,pool), 0700)
  return settings.TEMP_CONF_DIR

# make path to export qcows to
# FIXME do this for all pools from the main loop
def setup_qcow_temp_path(pool=''):
  if not pool:
    pool=settings.POOL
  logger = logs.get_logger()
  temp_path = settings.QCOW_TEMP_PATH
  if not os.path.isdir("%s/%s" % (temp_path,pool)):
    if settings.NOOP:
      logger.info('NOOP: would have made qcow temp path %s/%s' % (temp_path,pool))
    else:
      logger.info('creating qcow temp path: %s/%s' % (temp_path,pool))
      os.makedirs("%s/%s" % (temp_path,pool),0700)
  else:
    logger.info('using qcow temp path: %s/%s' % (temp_path,pool))

# check that temp_path is empty
# this is used to rotate orphans
# TODO make this use an empty tempdir
def make_empty_source():
  temp_path = "%s/empty_source/" % settings.QCOW_TEMP_PATH
  # get logger we setup earlier
  logger = logs.get_logger()
  try:
    dirlist = os.listdir(temp_path)
    if len(dirlist) != 0:
      raise NameError('temp_path_not_empty')
  except:
    if settings.NOOP:
      logger.info('NOOP: would have made temp empty source at %s' % temp_path)
    else:
      os.mkdir(temp_path,0700)
    # TODO catch if error

def setup_dir(directory):
  logger = logs.get_logger()
  # make the if it doesn't exist
  if not os.path.isdir(directory):
    # make dir and preceeding dirs if necessary
    if settings.NOOP:
      logger.info('NOOP: would have made directory %s' % directory)
    else:
      os.makedirs(directory,0700)

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
        logger.info('would have removed temp conf dirs %s/%s and %s',
          (settings.TEMP_CONF_DIR, settings.POOL, settings.TEMP_CONF_DIR))
      else:
        # TODO all pools
        os.rmdir("%s/%s" % (settings.TEMP_CONF_DIR, settings.POOL))
        os.rmdir(settings.TEMP_CONF_DIR)
    except Exception as e:
      logger.warning("unable to remove temp conf dir with error %s" % e)
