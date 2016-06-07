from ceph_rsnapshot import settings,logs
import tempfile, sys

import jinja2

def get_template():
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  # get path to this from setuptools
  f=open('rsnapshot.template','r')
  # FIXME raise error if error
  template_string = ''.join(f.readlines())
  f.close()
  template = Template(template_string)
  # FIXME raise error if error
  return template

def get_template_jinja():
  logger = logs.get_logger()
  env = jinja2.Environment(loader=jinja2.PackageLoader('ceph_rsnapshot'))
  template = env.get_template('rsnapshot.template')
  logger.info(template.render(nickname='foo'))


def write_conf(image,
               host,
               source='',
               # temp_path: note the . needed to set where to relative from
               temp_path='/tmp/qcows/./',
               backup_base_path='/backups/vms',
               conf_base_path='/etc/rsnapshot/vms',
               extra_args='',
               template = None):
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  # only reopen template if we don't have it - ie, are we part of a pool run
  if not template:
    template = get_template()
  # create source path string
  if source=='':
    source = 'root@%s:%s/%s.qcow2' % (host, temp_path, image)
  destination = '%s/%s/%s' % (backup_base_path, pool, image)
  logger.info('writing conf to rsnap %s to %s' % (source, destination))
  my_template = template.substitute(nickname = image,
                                    source = source,
                                    destination = destination,
                                    retain_interval = 'daily',
                                    retain_number = 14,
                                    subdir = '',
                                    extra_args = extra_args)
  conf_file = open('%s/%s.conf' % (conf_base_path, image),'w')
  # FIXME raise error if error
  conf_file.write(my_template)
  # FIXME raise error if error
  return conf_file.name

# mkstemp
# fdopen



def remove_conf(image,pool='rbd'):
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  os.remove('/etc/rsnapshot/vms/%s.conf' % image)
  # FIXME raise error if error



def setup_temp_conf_dir():
  logger = logs.get_logger()
  if settings.TEMP_CONF_DIR:
    if os.path.isdir(settings.TEMP_CONF_DIR):
      logger.info('using temp conf dir %s' % settings.TEMP_CONF_DIR)
      return settings.TEMP_CONF_DIR
    else:
      try:
        os.makedirs(settings.TEMP_CONF_DIR)
        logger.info('created temp dir at %s' % settings.TEMP_CONF_DIR)
      except Exception as e:
        logger.error('Cannot create conf temp dir (or intermediate dirs) from' +
        'setting %s with error %s' % (settings.TEMP_CONF_DIR, e))
        sys.exit(1)
  else:
    try:
      temp_conf_dir = tempfile.mkdtemp(prefix=settings.TEMP_CONF_DIR_PREFIX)
      # store this in global settings
      settings.TEMP_CONF_DIR = temp_conf_dir
      return temp_conf_dir
    except Exception as e:
      logger.error('cannot create conf temp dir with error %s' % e)
      sys.exit(1)


def test_template():
  settings.load_settings()
  settings.LOG_BASE_PATH='/tmp/ceph_rsnapshot_logs'
  logger=logs.setup_logging()
  temp_conf_dir = setup_temp_conf_dir()
  logger.info('temp conf dir is %s' % temp_conf_dir)
  print(settings.TEMP_CONF_DIR)

  get_template_jinja()