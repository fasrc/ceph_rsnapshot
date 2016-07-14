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
import json
import base64

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
  logger = logs.get_logger()
  if not pool:
    pool = settings.POOL
  host = settings.CEPH_HOST
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  # FIXME validate pool name no spaces?
  try:
    # TODO FIXME add a timeout here or the first connection and error differently if the source is not responding
    noopstring=''
    if settings.NOOP:
      noopstring='--noop'
    # now escape image_re to send
    # TODO NOTE this will now override any settings.IMAGE_RE specified
    # in yaml  config on the ceph node
    imagerebase64 = base64.encodestring(settings.IMAGE_RE).strip("\n")
    names_on_source_result = sh.ssh(host,
      '/home/ceph_rsnapshot/venv/bin/gathernames --pool "%s" --imagerebase64 %s'
      ' %s' % (pool, imagerebase64, noopstring))
    logger.info("log output from source node:\n"+names_on_source_result.stderr.strip("\n"))
  except sh.ErrorReturnCode as e:
    logger.error(e)
    logger.error('getting names failed with exit code: %s' % e.exit_code)
    logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
    logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
    # FIXME raise error to main loop
    raise NameError('get names on source failed with error: %s ' % e)
  names_on_source = names_on_source_result.strip("\n").split("\n")

  return names_on_source



def get_names_on_dest(pool=''):
  logger = logs.get_logger()
  if not pool:
    pool = settings.POOL
  backup_base_path=settings.BACKUP_BASE_PATH
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  backup_path = "%s/%s" % (backup_base_path, pool)
  try:
    names_on_dest = os.listdir(backup_path)
  except (IOError, OSError) as e:
    if settings.NOOP:
      # this will fail if noop and the dir doesn't exist, so
      # fake nothing there and move on
      logger.info('NOOP: would have listed vms in directory %s' % backup_path )
      return []
    logger.error(e)
    raise NameError('get_names_on_dest failed with error %s' % e)
  # FIXME cehck error
  return names_on_dest

# FIXME use same list of names on source
# UPDATE this is not used anymore. including in one last git commit for
# error handling tracking
def get_orphans_on_dest(pool=''):
  logger = logs.get_logger()
  if not pool:
    pool = settings.POOL
  backup_base_path = settings.BACKUP_BASE_PATH
  host = settings.CEPH_HOST
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  backup_path = "%s/%s" % (backup_base_path, pool)
  try:
    names_on_dest = get_names_on_dest(pool=pool)
  except NameError as e:
    logger.error('cannot get names on dest with error: %s' % e)
    # TODO what to do here? continue? retry?
    # if this errored, something is wrong with the backup directory. even if
    # it's empty this should have returned []
    # so fail run
    raise NameError('cannot get names on dest, failing run')
  try:
    names_on_source = get_names_on_source(pool=pool)
  except:
    logger.error('cannot get names from source with error %s' % e)
    # fail out
    raise NameError('cannot get names on source, failing run')
  orphans_on_dest = list(set(names_on_dest) - set(names_on_source))
  return orphans_on_dest




def rotate_orphans(pool=''):
  logger = logs.get_logger()
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
    try:
      dirs.make_empty_source() # do this every time to be sure it's empty
    except NameError as e:
      logger.error('error with creating or verifying temp empty source,'
        ' cannot rotate orphans. error: ' % e)
      # fail out
      return({'orphans_rotated': orphans_rotated, 'orphans_failed_to_rotate':
        [orphan for orphan in orphans_on_dest if orphan not in orphans_rotated]})
    # note this uses temp_path on the dest - which we check to be empty
    # note also create path with . in it so rsync relative works
    source = "%s/empty_source/./" % settings.QCOW_TEMP_PATH
    conf_file = write_conf(orphan,
                           pool = pool,
                           source = source,
                           template = template)
    logger.info("rotating orphan %s" % orphan)
    if settings.NOOP:
      logger.info('NOOP: would have rotated orphan here using rsnapshot conf see previous lines')
    else:
      try:
        rsnap_result = rsnapshot('-c','%s/%s/%s.conf' % (settings.TEMP_CONF_DIR, pool, orphan),settings.RETAIN_INTERVAL)
        # if ssuccessful, log
        if rsnap_result.stdout.strip("\n"):
          logger.info("successful; stdout from rsnap:\n"+rsnap_result.stdout.strip("\n"))
        orphans_rotated.append(orphan)
      except sh.ErrorReturnCode as e:
        orphans_failed_to_rotate.append(orphan)
        logger.error("failed to rotate orphan %s with code %s" % (orphan, e.exit_code))
        logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
        logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
    # unless flag to keep it for debug
    if not settings.KEEPCONF:
      remove_conf(orphan,pool)

  # TODO now check for any image dirs that are entirely empty and remove them (and the empty daily.NN inside them)
  return({'orphans_rotated': orphans_rotated, 'orphans_failed_to_rotate': orphans_failed_to_rotate})

def export_qcow(image,pool=''):
  logger = logs.get_logger()
  if not pool:
    pool = settings.POOL
  cephuser = settings.CEPH_USER
  cephcluster = settings.CEPH_CLUSTER
  # get logger we setup earlier
  logger.info("exporting %s" % image)
  try:
    noopstring=''
    if settings.NOOP:
      noopstring='--noop'
    logger.info("ceph host %s" % settings.CEPH_HOST)
    export_result = ssh(settings.CEPH_HOST,
      '/home/ceph_rsnapshot/venv/bin/export_qcow %s --pool %s --cephuser %s'
      ' --cephcluster %s %s' % (image, pool, cephuser, cephcluster, noopstring))
    export_qcow_ok = True
    logger.info("stdout from source node:\n"+export_result.stdout.strip("\n"))
  except sh.ErrorReturnCode as e:
    export_qcow_ok = False
    logger.error("failed to export qcow %s with code %s" % (image, e.exit_code))
    logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
    logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
  except Exception as e:
    # TODO
    logger.exception(e)
  return export_qcow_ok

def rsnap_image_sh(image,pool=''):
  logger = logs.get_logger()
  if not pool:
    pool = settings.POOL
  # TODO check free space before rsnapping
  logger.info("rsnapping %s" % image)
  rsnap_conf_file = '%s/%s/%s.conf' % (settings.TEMP_CONF_DIR, pool, image)
  if settings.NOOP:
    logger.info('NOOP: would have rsnapshotted image from conf file '
    '%s/%s/%s.conf for retain interval %s ' % (settings.TEMP_CONF_DIR,
      pool, image,settings.RETAIN_INTERVAL))
    # set this False so it's clear this wasn't successful as it was a noop
    rsnap_ok = False
  else:
    try:
      ts=time.time()
      rsnap_result = rsnapshot('-c', rsnap_conf_file, settings.RETAIN_INTERVAL)
      tf=time.time()
      elapsed_time = tf-ts
      elapsed_time_ms = elapsed_time * 10**3
      rsnap_ok = True
      logger.info("rsnap successful for image %s in %sms" % (image, elapsed_time_ms))
      if rsnap_result.stdout.strip("\n"):
        logger.info("stdout from rsnap:\n"+rsnap_result.stdout.strip("\n"))
    # TODO handle only rsnap sh exception
    except sh.ErrorReturnCode as e:
      logger.error("failed to rsnap %s with code %s" % (image, e.exit_code))
      # TODO move log formatting and writing to a function
      logger.error("stdout from rsnap:\n"+e.stdout.strip("\n"))
      logger.error("stderr from rsnap:\n"+e.stderr.strip("\n"))
      rsnap_ok = False
  return rsnap_ok

def remove_qcow(image,pool=''):
  logger = logs.get_logger()
  if not pool:
    pool = settings.POOL
  logger.info('going to remove temp qcow for image %s pool %s' % (image, pool))
  try:
    noopstring=''
    if settings.NOOP:
      noopstring=' --noop'
    remove_result = ssh(settings.CEPH_HOST,'/home/ceph_rsnapshot/venv/bin/remove_qcow %s --pool %s %s' % (image, pool, noopstring))
    remove_qcow_ok = True
    logger.info("stdout from source node:\n"+remove_result.stdout.strip("\n"))
  except sh.ErrorReturnCode as e:
    logger.error(e)
    logger.error("failed to remove qcow %s with code %s" % (image, e.exit_code))
    logger.error("stdout from source node:\n"+e.stdout.strip("\n"))
    logger.error("stderr from source node:\n"+e.stderr.strip("\n"))
    remove_qcow_ok = False
  return remove_qcow_ok

def rsnap_image(image, pool = '', template = None):
  if not pool:
    pool = settings.POOL
  # temp_path: note the . needed to set where to relative from
  temp_path=settings.QCOW_TEMP_PATH
  extra_args=settings.EXTRA_ARGS

  conf_base_path = settings.TEMP_CONF_DIR
  backup_base_path = settings.BACKUP_BASE_PATH

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
  if not settings.KEEPCONF:
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
  logger = logs.get_logger()
  logger.debug("starting rsnap of ceph pool %s to qcows in %s/%s" % (pool, settings.BACKUP_BASE_PATH, pool))

  # get list of images from source
  try:
    names_on_source = get_names_on_source(pool=pool)
  except:
    logger.error('cannot get names from source with error %s' % e)
    # fail out
    raise NameError('cannot get names on source, failing run')
  logger.info("names on source: %s" % ",".join(names_on_source))

  # get list of images on backup dest already
  try:
    names_on_dest_result=get_names_on_dest(pool = pool)
  except:
    logger.error('cannot get names from dest with error %s' % e)
    # fail out
    raise NameError('cannot get names on dest, failing run')
  logger.info("names on dest: %s" % ",".join(names_on_dest_result))

  # calculate difference
  orphans_on_dest = [image for image in names_on_dest if image not in names_on_source]
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
    # TODO decide if this is critical/stop or just warn
    logger.critical('no images found on source')
    # sys.exit(1)
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

# print current settings
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


# if not cli then check env

# enty for the rsnap node
def ceph_rsnapshot():
# if __name__=='__main__':
  parser = argparse.ArgumentParser(description='wrapper script to backup a ceph pool of rbd images to qcow',
                                   argument_default=argparse.SUPPRESS)
  parser.add_argument("-c", "--config", required=False, help="path to alternate config file")
  parser.add_argument("--host", required=False, help="ceph node to backup from")
  parser.add_argument('-p', '--pool', help='ceph pool to back up', required=False)
  parser.add_argument('--imagere', required=False, help='RE to match images to back up')
  parser.add_argument("-v", "--verbose", action='store_true',required=False, help="verbose logging output")
  parser.add_argument("--noop", action='store_true',required=False, help="noop - don't make any directories or do any actions. logging only to stdout")
  parser.add_argument("--printsettings", action='store_true',required=False, help="print out settings using and exit")
  parser.add_argument("-k", "--keepconf", action='store_true',required=False, help="keep conf files after run")
  parser.add_argument("-e", "--extralongargs", required=False, help="extra long args for rsync of format foo,bar for arg --foo --bar")
  # TODO add a param to show config it would use
  # to show names on source only
  # parser.add_argument('image_filter', help='regex to select rbd images to back up') # FIXME use this param not image_re  also FIXME pass this to gathernames? (need to shell escape it...)  have gathernames not do any filtering, so filter in this script, and then on the export qcow check if it has a snap
  args = parser.parse_args()

  # if we got passed an alt config file path, use that
  if args.__contains__('config'):
    config_file = args.config
    settings.load_settings(config_file)
  else:
    settings.load_settings()

  logger = setup_logging()
  logger.info("launched with cli args: " + " ".join(sys.argv))

  # override global settings with cli args
  # TODO get this working this way
  # for key in args.__dict__.keys():
  #   etc
  if args.__contains__('host'):
    settings.CEPH_HOST = args.host
  if args.__contains__('pool'):
    settings.POOL = args.pool
  if args.__contains__('verbose'):
    settings.VERBOSE = args.verbose
  if args.__contains__('noop'):
    settings.NOOP = args.noop
  if args.__contains__('keepconf'):
    settings.KEEPCONF = args.keepconf
  if args.__contains__('extralongargs'):
    settings.EXTRA_ARGS = ' '.join(['--'+x for x in args.extralongargs.split(',')])
    # FIXME not working correctly
  # image_filter = args.image_filter
  if args.__contains__('imagere'):
    settings.IMAGE_RE = args.imagere

  # print out settings using and exit
  if args.__contains__('printsettings'):
    # if it's there it's true
    logger.info('settings would have been:\n')
    logger.info(json.dumps(get_current_settings(), indent=2))
    logger.info('exiting')
    sys.exit(0)
  else:
    print('running with settings:\n')
    logger.info(json.dumps(get_current_settings(), indent=2))


  # TODO wrap pools here
  # for pool in POOLS:
  #   settings.POOL=pool

  # get local variables we need from settings we just set
  pool = settings.POOL

  # write lockfile for this pool
  # http://stackoverflow.com/a/789383/5928049
  pid = str(os.getpid())
  pidfile = ("/var/run/ceph_rsnapshot_cephhost_"
    "%s_pool_%s.pid""" ) % (settings.CEPH_HOST, pool)
  if os.path.isfile(pidfile):
    logger.error("pidfile %s already exists, exiting" % pidfile)
    sys.exit(1)
  logger.info("writing lockfile at %s" % pidfile)
  file(pidfile, 'w').write(pid)
  try:
    # we've made the lockfile, so rsnap the pool
    # setup directories
    dirs.setup_temp_conf_dir(pool)
    dirs.setup_backup_dirs()
    dirs.setup_log_dirs()

    try:
      result = rsnap_pool(pool)
    except NameError as e:
      # TODO get some way to still have the list of images that it completed
      # before failing
      logger.error('rsnap pool %s failed error: %s' % (pool, e))
      raise

    if not settings.KEEPCONF:
      dirs.remove_temp_conf_dir()

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
  finally:
    # done with this pool so clear the pidfile
    logger.info("removing lockfile at %s" % pidfile)
    os.unlink(pidfile)

    if settings.NOOP:
      logger.info("end of NOOP run")

    # TODO should these still sys.exit or should they let the exceptions go?
    if result['failed']:
      sys.exit(1)
    elif result['orphans_failed_to_rotate']:
      sys.exit(2)
    else:
      sys.exit(0)

# Nothing else down here - it all goes in the finally
