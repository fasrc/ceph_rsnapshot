from ceph_rsnapshot import settings, logs
import os


def setup_backup_dirs(dirs=''):
  if not dirs:
    dirs = [ settings.BACKUP_BASE_PATH,
             "%s/%s" % (settings.BACKUP_BASE_PATH, pool),
           ]
  # TODO support multiple pools
  # for pool in settings.POOLS
  #   dirs.append(pool)
  for directory in dirs:
    setup_dir(directory)

def setup_log_dirs():
  dirs = [
      settings.LOG_BASE_PATH,
      "%s/rsnap" % settings.LOG_BASE_PATH,
      "%s/rsnap/%s" % (settings.LOG_BASE_PATH, pool),
    ]
  for directory in dirs:
    setup_dir(directory)

def setup_temp_conf_dir(pool):
  logger = logs.get_logger()
  if settings.TEMP_CONF_DIR:
    if os.path.isdir(settings.TEMP_CONF_DIR):
      logger.info('using temp conf dir %s' % settings.TEMP_CONF_DIR)
    else:
      try:
        os.makedirs(settings.TEMP_CONF_DIR)
        logger.info('created temp dir at %s' % settings.TEMP_CONF_DIR)
      except Exception as e:
        logger.error('Cannot create conf temp dir (or intermediate dirs) from'+
        ' setting %s with error %s' % (settings.TEMP_CONF_DIR, e))
        sys.exit(1)
  else:
    try:
      temp_conf_dir = tempfile.mkdtemp(prefix=settings.TEMP_CONF_DIR_PREFIX)
      # store this in global settings
      settings.TEMP_CONF_DIR = temp_conf_dir
    except Exception as e:
      logger.error('cannot create conf temp dir with error %s' % e)
      sys.exit(1)
  logger.info('creating temp conf subdir for pool %s' % pool)
  os.mkdir("%s/%s" % (settings.TEMP_CONF_DIR,pool), 700)
  return settings.TEMP_CONF_DIR

# make path to export qcows to
def setup_qcow_temp_path(pool):
  logger = logs.get_logger()
  temp_path = settings.QCOW_TEMP_PATH
  if not os.path.isdir(temp_path):
    os.mkdir("%s/%s" % (temp_path,pool),700)
  if not os.path.isdir("%s/%s" % (temp_path,pool)):
    os.mkdir("%s/%s" % (temp_path,pool),700)

# check that temp_path is empty
# this is used to rotate orphans
def make_empty_source():
  temp_path = settings.QCOW_TEMP_PATH
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  try:
    dirlist = os.listdir(temp_path)
    if len(dirlist) != 0:
      raise NameError('temp_path_not_empty')
  except:
    os.mkdir(temp_path,700)
    # TODO catch if error

def setup_dir(directory):
  # make the if it doesn't exist
  if not os.path.isdir(directory):
    os.mkdir(directory,700)

def setup_dir_per_pool(directory):
  # for pool in settings.POOLS
  #   dirs.append(pool)
  #   setup_dir(dirs)
  pass