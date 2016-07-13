
import os
import sys
import tempfile

import yaml, logging

# first one found in this list is the one used
DEFAULT_CONFIG_HIERARCHY = [
  'ceph_rsnapshot.yaml',
  '/home/ceph_rsnapshot/config/ceph_rsnapshot.yaml',
]


SETTINGS = dict(
  CEPH_HOST='localhost',
  CEPH_USER='admin',
  CEPH_CLUSTER='ceph',
  # TODO allow an array here
  POOL='rbd',
  # path for the temporary export of qcows
  QCOW_TEMP_PATH='/tmp/qcows/',
  EXTRA_ARGS='',
  # prefix for temp dir to store temporary rsnapshot conf files
  TEMP_CONF_DIR_PREFIX='ceph_rsnapshot_temp_conf_',
  # or can override and set whole dir
  TEMP_CONF_DIR='',
  BACKUP_BASE_PATH='/backups/vms',
  KEEPCONF=False,
  LOG_BASE_PATH='/var/log/ceph_rsnapshot',
  LOG_FILENAME='ceph_rsnapshot.log',
  # TODO allow specifying alt path to a jinja template
  # TEMPLATE = '',
  VERBOSE=False,
  NOOP=False,
  # opennebula images are one-NN
  # vms are one-NN-XX-YY for image NN vm XX and disk YY
  # images or vms are (with the additional accidental acceptance of one-NN-XX
  # RE to filter ceph rbd images to back up
  IMAGE_RE=r'^one(-[0-9]+){1,3}$',
  RETAIN_INTERVAL='daily',
  RETAIN_NUMBER=14,
  # date format string to pass to `date` to get snap naming,
  # iso format %Y-%m-%d would yield names like imagename@2016-10-04
  # TODO use this everywhere instead of date --iso
  SNAP_NAMING_DATE_FORMAT='%Y-%m-%d',
  # min freespace to leave on ceph node for exporting qcow temporarily
  MIN_FREESPACE = 100*1024*1024, # 100mb
  SH_LOGGING=False,

)


def load_settings(config_file=''):
  logger = logging.getLogger('ceph_rsnapshot')
  if config_file=='':
    for conf_file in DEFAULT_CONFIG_HIERARCHY:
      if os.path.isfile(conf_file):
        config_file = conf_file
        break
  settings = SETTINGS.copy()
  if os.path.isfile(config_file):
    with open(config_file) as f:
      cfg = yaml.load(f.read()) or {}
    for setting in cfg:
      if setting.upper() not in SETTINGS:
        logger.error('ERROR: unsupported setting %s\n' % setting)
        sys.exit(1)
      else:
        if setting.upper() == 'TEMP_CONF_DIR':
          # FIXME make sure KEEPCONF is enabled
          pass
        settings[setting.upper()] = cfg[setting]
  else:
    logger.info('WARNING: not loading config - using default settings')
  globals().update(settings)
  return settings

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
        IMAGE_RE=settings.IMAGE_RE,
        RETAIN_INTERVAL=settings.RETAIN_INTERVAL,
        RETAIN_NUMBER=settings.RETAIN_NUMBER,
        SNAP_NAMING_DATE_FORMAT=settings.SNAP_NAMING_DATE_FORMAT,
        MIN_FREESPACE=settings.MIN_FREESPACE, 
        SH_LOGGING=settings.SH_LOGGING,
    ))