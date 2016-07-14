#!/usr/bin/env python
# deletes a temp qcow

import os, sys, socket, logging
import sh
import argparse

from ceph_rsnapshot import logs
from ceph_rsnapshot import settings

def remove_qcow():
  parser = argparse.ArgumentParser(description='deletes a temporary qcow',
                                   argument_default=argparse.SUPPRESS)
  parser.add_argument('image')
  parser.add_argument('--pool', required=False)
  parser.add_argument("--noop", action='store_true',required=False, help="noop - don't make any directories or do any actions. logging only to stdout")
  # parser.add_argument('--sum', dest='accumulate', action='store_const',
  #                     const=sum, default=max,
  #                     help='sum the integers (default: find the max)')
  args = parser.parse_args()
  image = args.image

  settings.load_settings()

  if args.__contains__('pool'):
    settings.POOL = args.pool
  if args.__contains__('noop'):
    settings.NOOP = args.noop

  logger = logs.setup_logging()

  logger.info("deleting temp qcow for image %s from pool %s " % (image, settings.POOL))
  temp_qcow_file = "%s/%s/%s.qcow2" % (settings.QCOW_TEMP_PATH, settings.POOL, image)
  try:
    if settings.NOOP:
      logger.info('NOOP: would have removed temp qcow %s/%s/%s.qcow2' % (settings.QCOW_TEMP_PATH, settings.POOL, image))
    else:
      os.remove(temp_qcow_file)
  except (OSError, IOError) as e:
    logger.error('error removing temp qcow %s' % temp_qcow_file)
    logger.error(e)
    sys.exit(1)
  logger.info("successfully removed qcow for %s" % image)
