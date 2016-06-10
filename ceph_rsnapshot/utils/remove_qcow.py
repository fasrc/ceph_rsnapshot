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
  # parser.add_argument('--sum', dest='accumulate', action='store_const',
  #                     const=sum, default=max,
  #                     help='sum the integers (default: find the max)')
  args = parser.parse_args()
  image = args.image

  if args.__contains__('pool'):
    settings.POOL = args.pool

  settings.load_settings()

  logger = logs.setup_logging()

  logger.info("deleting temp qcow for image %s from pool %s " % (image, settings.POOL))

  try:
    os.remove("%s/%s/%s.qcow2" % (settings.QCOW_TEMP_PATH, settings.POOL, image))
  except Exception as e:
    logger.error(e.stdout)
    logger.error(e.stderr)
    raise NameError, e
  logger.info("successfully removed qcow for %s" % image)
