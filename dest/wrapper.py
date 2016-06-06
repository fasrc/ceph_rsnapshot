#!/usr/bin/env python

import sh
from sh import rsnapshot
from sh import echo
from sh import ssh
import os
import sys
from string import Template

import argparse, logging, socket,time
import re

image_re = r'^one\(-[0-9]\+\)\{1,2\}$'
# note using . in middle to tell rsnap where to base relative
temp_path = '/tmp/qcows/./'

sh_logging = False

def get_names_on_source(host, pool):
  # FIXME validate pool name no spaces?
  try:
    names_on_source_result = sh.ssh(host,'source venv/bin/activate; ./gathernames.py "%s"' % pool)
    logger.info("log output from source node:\n"+names_on_source_result.stderr.strip("\n"))
  except Exception as e:
    logger.error(e)
    logger.error('getting names failed with exit code: %s' % e.exit_code)
    logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
    logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
    # FIXME raise error to main loop
  names_on_source = names_on_source_result.strip("\n").split("\n")

  return names_on_source

def get_template():
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

def remove_conf(image,pool='rbd'):
  os.remove('/etc/rsnapshot/vms/%s.conf' % image)
  # FIXME raise error if error

def get_names_on_dest(pool='rbd',backup_base_path='/backups/vms'):
  backup_path = "%s/%s" % (backup_base_path, pool)
  try:
    names_on_dest = os.listdir(backup_path)
  except Exception as e:
    logger.error(e)
    raise NameError('get_names_on_dest failed')
  # FIXME cehck error
  return names_on_dest

# FIXME use same list of names on source
def get_orphans_on_dest(host, pool='rbd',backup_base_path='/backups/vms'):
  backup_path = "%s/%s" % (backup_base_path, pool)
  names_on_dest = get_names_on_dest(pool,backup_base_path)
  names_on_source = get_names_on_source(host=host,pool=pool)
  orphans_on_dest = list(set(names_on_dest) - set(names_on_source))
  return orphans_on_dest

# check that temp_path is empty
def make_empty_source():
  try:
    dirlist = os.listdir(temp_path)
    if len(dirlist) != 0:
      raise NameError('temp_path_not_empty')
  except:
    os.mkdir(temp_path,0700)


def rotate_orphans(pool='rbd',backup_base_path = '/backups/vms', conf_base_path = '/etc/rsnapshot/vms'):
  # now check for ophans on dest
  backup_path = "%s/%s" % (backup_base_path, pool)
  orphans_on_dest = get_orphans_on_dest(pool, backup_path)

  orphans_rotated = []
  orphans_failed_to_rotate = []

  for orphan in orphans_on_dest:
    logger.info('orphan: %s' % orphan)
    make_empty_source() #FIXME do this only once
    # note this uses temp_path on the dest - which we check to be empty
    conf_file = write_conf(orphan,
                           source = temp_path,
                           conf_base_path = conf_base_path,
                           backup_base_path = backup_base_path)
    logger.info("rotating orphan %s" % orphan)
    try:
      rsnap_result = rsnapshot('-c','%s/%s.conf' % (conf_base_path, orphan),'daily')
      # if ssuccessful, log
      orphans_rotated.append(orphan)
    except Exception as e:
      orphans_failed_to_rotate.append(orphan)
      logger.error("failed to rotate orphan %s with code %s" % (orphan, e.exit_code))
      logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
      logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
    remove_conf(orphan,pool)
  return({'orphans_rotated': orphans_rotated, 'orphans_failed_to_rotate': orphans_failed_to_rotate})

def setup_logging(log_location='/var/log/ceph-rsnapshot/', log_filename='ceph-rsnapshot', verbose=False):
  logger = logging.getLogger(__name__)
  # get logger for sh module so we can configure it as well
  sh_logger = logging.getLogger('sh.command')

  log_level = logging.INFO
  if verbose == True:
    log_level = logging.DEBUG
  if not os.path.isdir(log_location):
    os.makedirs(log_location)
  log_file = "%s/%s.log" % (log_location, log_filename)
  # logging.basicConfig(filename=log_file, level=log_level)

  logger.setLevel(log_level)
  if sh_logging:
    sh_logger.setLevel(log_level)

  # setup log format
  pid = os.getpid()
  hostname = socket.gethostname()
  LOG_FORMAT = ("%(asctime)s [" + hostname + " PID:" + str(pid) +
                "] [%(levelname)-5.5s] [%(name)s] %(message)s")
  # LOG_FORMAT = ("%(asctime)s [" + str(pid) +
  #               "] [%(levelname)-5.5s] [%(name)s] %(message)s")
  logFormatter = logging.Formatter(LOG_FORMAT)

  # set format on file loggers
  fileHandler = logging.FileHandler(log_file)
  fileHandler.setFormatter(logFormatter)
  logger.addHandler(fileHandler)
  if sh_logging:
    sh_logger.addHandler(fileHandler)

  # setup console loggers
  consoleHandler = logging.StreamHandler(stream=sys.stdout)
  consoleHandler.setFormatter(logFormatter)
  logger.addHandler(consoleHandler)
  if sh_logging:
    sh_logger.addHandler(consoleHandler)
  return(logger)

def export_qcow(image,host,pool='rbd'):
  logger.info("exporting %s" % image)
  try:
    # TODO add a dry-run option
    export_result = ssh(host,'source venv/bin/activate; ./export_qcow.py %s' % image)
    export_qcow_ok = True
    logger.info("stdout from source node:\n"+export_result.stdout.strip("\n"))
  except Exception as e:
    export_qcow_ok = False
    logger.error("failed to export qcow %s with code %s" % (image, e.exit_code))
    logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
    logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
  return export_qcow_ok

def rsnap_image_sh(image,host,pool='rbd',conf_base_path='/etc/rsnapshot/vms'):
  logger.info("rsnapping %s" % image)
  try:
    ts=time.time()
    rsnap_result = rsnapshot('-c','%s/%s.conf' % (conf_base_path, image),'daily')
    tf=time.time()
    elapsed_time = tf-ts
    elapsed_time_ms = elapsed_time * 10**3
    rsnap_ok = True
    logger.info("rsnap successful for image %s in %sms" % (image, elapsed_time_ms))
    if rsnap_result.stdout.strip("\n"):
      logger.info("stdout from rsnap:\n"+rsnap_result.stdout.strip("\n"))
  except Exception as e:
    logger.error("failed to rsnap %s with code %s" % (image, e.exit_code))
    # TODO move log formatting and writing to a function
    logger.error("stdout from rsnap:\n"+e.stdout.strip("\n"))
    logger.error("stderr from rsnap:\n"+e.stderr.strip("\n"))
    rsnap_ok = False
  return rsnap_ok

def remove_qcow(image,host,pool='rbd'):
  try:
    remove_result = ssh(host,'source venv/bin/activate; ./remove_qcow.py %s' % image)
    remove_qcow_ok = True
    logger.info("stdout from source node:\n"+remove_result.stdout.strip("\n"))
  except Exception as e:
    logger.error("failed to remove qcow %s with code %s" % (image, e.exit_code))
    logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
    logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
    remove_qcow_ok = False
  return remove_qcow_ok

def rsnap_image(image,
                host,
                pool = 'rbd',
                # temp_path: note the . needed to set where to relative from
                temp_path='/tmp/qcows/./',
                extra_args='',
                template = None,
                conf_base_path = '/etc/rsnapshot/vms',
                backup_base_path = '/backups/vms',
                keepconf = False):
  logger.info('working on image %s' % image)
  # setup flags
  export_qcow_ok = False
  rsnap_ok = False
  remove_qcow_ok = False
  # create backup path from base and image name
  backup_path = "%s/%s" % (backup_base_path, pool)
  # only reopen if we haven't pulled this yet - ie, are we part of a pool run
  if not template:
    template = get_template()
  # create the temp conf file
  conf_file = write_conf(image,
                         host = host,
                         temp_path = temp_path,
                         backup_base_path = backup_base_path,
                         conf_base_path = conf_base_path,
                         extra_args = extra_args,
                         template = template)
  logger.info(conf_file)

  # ssh to source and export temp qcow of this image
  export_qcow_ok = export_qcow(image, host=host)

  # if exported ok, then rsnap this image
  if export_qcow_ok:
    rsnap_ok = rsnap_image_sh(image, host=host)
  else:
    logger.error("skipping rsnap of image %s because export to qcow failed" % image)

  # either way remove the temp qcow
  logger.info("removing %s" % image)
  remove_qcow_ok = remove_qcow(image, host=host)
  # TODO catch error if its already gone and move forward

  # either way remove the temp conf file
  # unless flag to keep it for debug
  if not keepconf:
    remove_conf(image)

  if export_qcow_ok and rsnap_ok and remove_qcow_ok:
    successful = True
  else:
    successful = False
  # return a blob with the details
  return({'image': image,
          'pool': pool,
          'successful': successful,
          'status': {
                     'export_qcow_ok': export_qcow_ok,
                     'rsnap_ok': rsnap_ok,
                     'remove_qcow_ok': remove_qcow_ok
                   }
         })

def rsnap_pool(host,
               pool,
               # temp_path: note the . needed to set where to relative from
               temp_path='/tmp/qcows/./',
               extra_args='',
               template = None,
               conf_base_path = '/etc/rsnapshot/vms',
               backup_base_path = '/backups/vms',
               keepconf = False):

  # start run
  logger.debug("starting rsnap of ceph pool %s to qcows in %s/%s" % (pool, backup_base_path, pool))

  # get list of images from source
  names_on_source = get_names_on_source(host=host,pool=pool)
  # TODO handle errors here
  logger.info("names on source: %s" % ",".join(names_on_source))

  # get list of images on backup dest already
  names_on_dest_result=get_names_on_dest(pool = pool, backup_base_path = backup_base_path)
  logger.info("names on dest: %s" % ",".join(names_on_dest_result))

  # calculate difference
  orphans_on_dest = get_orphans_on_dest(host = host, pool = pool, backup_base_path = backup_base_path)
  if orphans_on_dest:
    logger.info("orphans on dest: %s" % ",".join(orphans_on_dest))

  # get template string for rsnap conf
  template = get_template()

  successful = []
  failed = {}
  orphans_rotated = []
  orphans_failed_to_rotate = []

  len_names = len(names_on_source)
  index = 1
  if len_names == 1 and names_on_source[0] == u'':
    logger.critical('no images found on source')
    sys.exit(1)
  else:
    for image in names_on_source:
      logger.info('working on name %s of %s in pool %s: %s' % (index, len_names, pool, image))

      result = rsnap_image(image,
                host = host,
                pool = pool,
                # temp_path: note the . needed to set where to relative from
                temp_path = temp_path,
                extra_args = extra_args,
                template = template,
                conf_base_path = conf_base_path,
                backup_base_path = backup_base_path,
                keepconf = keepconf)

      if result['successful']:
        logger.info('successfully done with %s' % image)
        # store in array
        successful.append(image)
      else:
        logger.error('error on %s' % image)
        # store dict in dict
        failed[image] = result
      # done with this image, increment counter
      index = index + 1


  return({'successful': successful,
          'failed': failed,
          'orphans_rotated': orphans_rotated,
          'orphans_failed_to_rotate': orphans_failed_to_rotate,
        })




if __name__=='__main__':
  parser = argparse.ArgumentParser(description='wrapper script to backup a ceph pool of rbd images to qcow')
  parser.add_argument("--host", required=True, help="ceph node to backup from")
  parser.add_argument('-p', '--pool', help='ceph pool to back up', required=False, default='rbd')
  parser.add_argument("-v", "--verbose", action='store_true',required=False, default=False,help="verbose logging output")
  parser.add_argument("-k", "--keepconf", action='store_true',required=False, default=False,help="keep conf files after run")
  parser.add_argument("-e", "--extralongargs", required=False, default='',help="extra long args for rsync of format foo,bar for arg --foo --bar")
  # parser.add_argument('image_filter', help='regex to select rbd images to back up') # FIXME use this param not image_re  also FIXME pass this to gathernames? (need to shell escape it...)  have gathernames not do any filtering, so filter in this script, and then on the export qcow check if it has a snap
  args = parser.parse_args()
  host = args.host
  pool = args.pool
  verbose = args.verbose
  keepconf = args.keepconf
  extra_args = ''
  if args.extralongargs:
    extra_args = ' '.join(['--'+x for x in args.extralongargs.split(',')])
    # FIXME not working correctly
  # image_filter = args.image_filter

  logger = setup_logging(verbose=verbose)
  logger.debug(" ".join(sys.argv))

  result = rsnap_pool(host=host,pool=pool,keepconf=keepconf,extra_args = extra_args)

  # write output
  logger.info("Successful: %s" % ','.join(result['successful']))
  if result['failed']:
    logger.error("Failed:")
    logger.error(result['failed'])
  if result['orphans_rotated']:
    logger.info("orphans rotated:")
    logger.info(result['orphans_rotated'])
  if result['orphans_failed_to_rotate']:
    logger.error("orphans failed to rotate:")
    logger.error(result['orphans_failed_to_rotate'])
  logger.info("done")

  if result['failed']:
    sys.exit(1)
  elif result['orphans_failed_to_rotate']:
    sys.exit(2)
  else:
    sys.exit(0)
