

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




def test_template():
  print('foo')
