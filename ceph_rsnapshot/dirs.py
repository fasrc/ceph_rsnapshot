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

def setup_qcow_source_dirs():
  dirs = [
      settings.QCOW_TEMP_PATH,
      "%s/%s" % (settings.QCOW_BASE_PATH, pool),
    ]
  for directory in dirs:
    setup_dir(directory)

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
    os.mkdir(temp_path,0700)
    # TODO catch if error

def setup_dir(directory):
  # make the if it doesn't exist
  if not os.path.isdir(directory):
    os.mkdir(directory,0700)

def setup_dir_per_pool(directory):
  # for pool in settings.POOLS
  #   dirs.append(pool)
  #   setup_dir(dirs)