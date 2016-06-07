#!/usr/bin/env python

import sh
from sh import rsnapshot
from sh import echo
from sh import ssh
import os
import sys
from string import Template

import argparse, socket,time
import re

import logging

# from ceph_rsnapshot.helpers import qcow
# qcow.remove

from ceph_rsnapshot.logs import setup_logging
from ceph_rsnapshot import logs
from ceph_rsnapshot.templates import remove_conf, write_conf, get_template
# FIXME do imports this way not the above
from ceph_rsnapshot import settings, templates, dirs


image_re = r'^one\(-[0-9]\+\)\{1,2\}$'
# note using . in middle to tell rsnap where to base relative
temp_path = '/tmp/qcows/./'

def get_names_on_source(pool=''):
  if not pool:
    pool = settings.POOL
  host = settings.CEPH_HOST
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  # FIXME validate pool name no spaces?
  try:
    names_on_source_result = sh.ssh(host,'source venv_ceph_rsnapshot/bin/activate; gathernames "%s"' % pool)
    logger.info("log output from source node:\n"+names_on_source_result.stderr.strip("\n"))
  except Exception as e:
    logger.error(e)
    logger.error('getting names failed with exit code: %s' % e.exit_code)
    logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
    logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
    # FIXME raise error to main loop
  names_on_source = names_on_source_result.strip("\n").split("\n")

  return names_on_source



def get_names_on_dest(pool=''):
  if not pool:
    pool = settings.POOL
  backup_base_path=settings.BACKUP_BASE_PATH
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  backup_path = "%s/%s" % (backup_base_path, pool)
  try:
    names_on_dest = os.listdir(backup_path)
  except Exception as e:
    logger.error(e)
    raise NameError('get_names_on_dest failed')
  # FIXME cehck error
  return names_on_dest

# FIXME use same list of names on source
def get_orphans_on_dest(pool=''):
  if not pool:
    pool = settings.POOL
  backup_base_path = settings.BACKUP_BASE_PATH
  host = settings.CEPH_HOST
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  backup_path = "%s/%s" % (backup_base_path, pool)
  names_on_dest = get_names_on_dest(pool=pool,backup_base_path=backup_base_path)
  names_on_source = get_names_on_source(pool=pool)
  orphans_on_dest = list(set(names_on_dest) - set(names_on_source))
  return orphans_on_dest




def rotate_orphans(pool=''):
  if not pool:
    pool = settings.POOL
  backup_base_path = settings.BACKUP_BASE_PATH

  # now check for ophans on dest
  backup_path = "%s/%s" % (backup_base_path, pool)
  orphans_on_dest = get_orphans_on_dest(pool=pool)

  orphans_rotated = []
  orphans_failed_to_rotate = []

  template = get_template()

  for orphan in orphans_on_dest:
    logger.info('orphan: %s' % orphan)
    dirs.make_empty_source() # do this every time to be sure it's empty
    # note this uses temp_path on the dest - which we check to be empty
    conf_file = write_conf(orphan,
                           pool = pool,
                           source = settings.QCOW_TEMP_PATH,
                           template = template)
    logger.info("rotating orphan %s" % orphan)
    try:
      rsnap_result = rsnapshot('-c','%s/%s/%s.conf' % (settings.TEMP_CONF_DIR, pool, orphan),settings.RETAIN_INTERVAL)
      # if ssuccessful, log
      orphans_rotated.append(orphan)
    except Exception as e:
      orphans_failed_to_rotate.append(orphan)
      logger.error("failed to rotate orphan %s with code %s" % (orphan, e.exit_code))
      logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
      logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
    remove_conf(orphan,pool)
  return({'orphans_rotated': orphans_rotated, 'orphans_failed_to_rotate': orphans_failed_to_rotate})

def export_qcow(image,pool=''):
  if not pool:
    pool = settings.POOL
  cephuser = settings.CEPH_USER
  cephcluster = settings.CEPH_CLUSTER
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  logger.info("exporting %s" % image)
  try:
    # TODO add a dry-run option
    logger.info("ceph host %s" % settings.CEPH_HOST)
    export_result = ssh(settings.CEPH_HOST,'source venv_ceph_rsnapshot/bin/activate; export_qcow %s --pool %s --cephuser %s --cephcluster %s' % (image, pool, cephuser, cephcluster))
    export_qcow_ok = True
    logger.info("stdout from source node:\n"+export_result.stdout.strip("\n"))
  except Exception as e:
    export_qcow_ok = False
    logger.error("failed to export qcow %s with code %s" % (image, e.exit_code))
    logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
    logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
  return export_qcow_ok

def rsnap_image_sh(image,pool=''):
  if not pool:
    pool = settings.POOL

  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  logger.info("rsnapping %s" % image)
  try:
    rsnap_conf_file = '%s/%s/%s.conf' % (settings.TEMP_CONF_DIR, pool, image)
    ts=time.time()
    rsnap_result = rsnapshot('-c', rsnap_conf_file, settings.RETAIN_INTERVAL)
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

def remove_qcow(image,pool=''):
  if not pool:
    pool = settings.POOL

  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  logger.info('going to remove temp qcow for image %s pool %s' % (image, pool))
  try:
    remove_result = ssh(settings.CEPH_HOST,'source venv_ceph_rsnapshot/bin/activate; remove_qcow %s --pool %s' % (image, pool))
    remove_qcow_ok = True
    logger.info("stdout from source node:\n"+remove_result.stdout.strip("\n"))
  except Exception as e:
    logger.error(e)
    logger.error("failed to remove qcow %s with code %s" % (image, e.exit_code))
    logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
    logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
    remove_qcow_ok = False
  return remove_qcow_ok

def rsnap_image(image, pool = 'rbd', template = None):
  # temp_path: note the . needed to set where to relative from
  temp_path=settings.QCOW_TEMP_PATH
  extra_args=settings.EXTRA_ARGS

  conf_base_path = settings.TEMP_CONF_DIR
  backup_base_path = settings.BACKUP_BASE_PATH
  keepconf = settings.KEEPCONF

  # get logger we setup earlier
  logger = logs.get_logger()
  logger.info('working on image %s in pool %s' % (image, pool))
  # setup flags
  export_qcow_ok = False
  rsnap_ok = False
  remove_qcow_ok = False

  # only reopen if we haven't pulled this yet - ie, are we part of a pool run
  if not template:
    template = get_template()

  # create the temp conf file
  conf_file = write_conf(image, pool = pool, template = template)
  logger.info(conf_file)

  # ssh to source and export temp qcow of this image
  export_qcow_ok = export_qcow(image, pool = pool)

  # if exported ok, then rsnap this image
  if export_qcow_ok:
    rsnap_ok = rsnap_image_sh(image, pool = pool)
  else:
    logger.error("skipping rsnap of image %s because export to qcow failed" % image)

  # either way remove the temp qcow
  logger.info("removing temp qcow for %s" % image)
  remove_qcow_ok = remove_qcow(image, pool= pool)
  # TODO catch error if its already gone and move forward

  # either way remove the temp conf file
  # unless flag to keep it for debug
  if not keepconf:
    remove_conf(image, pool = pool)

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

def rsnap_pool(pool):
  # get values from settings
  host = settings.CEPH_HOST

  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  logger.debug("starting rsnap of ceph pool %s to qcows in %s/%s" % (pool, settings.BACKUP_BASE_PATH, pool))

  # get list of images from source
  names_on_source = get_names_on_source(pool=pool)
  # TODO handle errors here
  logger.info("names on source: %s" % ",".join(names_on_source))

  # get list of images on backup dest already
  names_on_dest_result=get_names_on_dest(pool = pool)
  logger.info("names on dest: %s" % ",".join(names_on_dest_result))

  # calculate difference
  # FIXME use the above lists
  orphans_on_dest = get_orphans_on_dest(pool = pool)
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

      result = rsnap_image(image, pool = pool, template = template)

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

  # {'orphans_rotated': orphans_rotated, 'orphans_failed_to_rotate': orphans_failed_to_rotate}
  orphan_result = rotate_orphans(pool)

  return({'successful': successful,
          'failed': failed,
          'orphans_rotated': orphan_result['orphans_rotated'],
          'orphans_failed_to_rotate': orphan_result['orphans_failed_to_rotate'],
        })


# if not cli then check env

# enty for the rsnap node
def ceph_rsnapshot():
# if __name__=='__main__':
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

  # FIXME override settings from cli args
  settings.load_settings()

  # override global settings with cli args
  settings.CEPH_HOST = host
  settings.POOL = pool
  settings.VERBOSE = verbose
  settings.EXTRA_ARGS = extra_args
  settings.KEEPCONF = keepconf

  logger = setup_logging()
  logger.debug("launched with cli args: " + " ".join(sys.argv))

  # TODO move this to dirs
  dirs.setup_temp_conf_dir(pool)
  dirs.setup_backup_dirs()
  dirs.setup_log_dirs()

  # TODO wrap pools here
  # for pool in POOLS:
  #   settings.POOL=pool
  result = rsnap_pool(pool)

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


