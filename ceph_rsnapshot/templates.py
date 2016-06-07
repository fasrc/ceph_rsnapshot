from ceph_rsnapshot import settings,logs
import tempfile, sys, os

import jinja2

def get_template():
  logger = logs.get_logger()
  env = jinja2.Environment(loader=jinja2.PackageLoader('ceph_rsnapshot'))
  template = env.get_template('rsnapshot.template')
  return template

def write_conf(image, pool = '', source='', template=''):
  if not pool:
    pool = settings.POOL
  host = settings.CEPH_HOST
  # temp_path: note the . needed to set where to relative from
  temp_path=settings.QCOW_TEMP_PATH
  backup_base_path=settings.BACKUP_BASE_PATH

  # get logger we setup earlier
  logger = logs.get_logger()

  # only reopen template if we don't have it - ie, are we part of a pool run
  if not template:
    template = get_template()

  # create source path string if an override wasn't passed to us
  # set the . to get rsync to do relative from there
  if source=='':
    source = 'root@%s:%s/%s/./%s.qcow2' % (settings.CEPH_HOST, settings.QCOW_TEMP_PATH, pool, image)

  destination = '%s/%s/%s' % (settings.BACKUP_BASE_PATH, settings.POOL, image)

  logger.info('writing conf for image %s to rsnap from %s to %s' % (image, source, destination))

  my_template = template.render(nickname = image,
                                source = source,
                                destination = destination,
                                retain_interval = settings.RETAIN_INTERVAL,
                                retain_number = settings.RETAIN_NUMBER,
                                log_base_path = settings.LOG_BASE_PATH,
                                subdir = '',
                                extra_args = settings.EXTRA_ARGS)

  # conf file of the form /tmp_conf_dir/pool/imagename.conf
  conf_file = open('%s/%s/%s.conf' % (settings.TEMP_CONF_DIR, pool, image),'w')
  # FIXME raise error if error
  conf_file.write(my_template)
  # FIXME raise error if error
  return conf_file.name

# note using mkdtemp to make a tmep dir for these so not using mkstemp
# mkstemp
# fdopen



def remove_conf(image,pool='rbd'):
  # get logger we setup earlier
  logger = logs.get_logger()
  os.remove('%s/%s.conf' % (settings.TEMP_CONF_DIR, image))
  # FIXME raise error if error



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
  logger.info('creading temp conf subdir for pool %s' % pool)
  os.mkdir("%s/%s" % (settings.TEMP_CONF_DIR,pool), 0700)
  return settings.TEMP_CONF_DIR


def test_template():
  settings.load_settings()
  settings.LOG_BASE_PATH='/tmp/ceph_rsnapshot_logs'
  logger=logs.setup_logging()
  temp_conf_dir = setup_temp_conf_dir()
  logger.info('temp conf dir is %s' % temp_conf_dir)
  print(settings.TEMP_CONF_DIR)

  get_template()