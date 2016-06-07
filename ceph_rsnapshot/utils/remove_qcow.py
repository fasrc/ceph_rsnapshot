#!/usr/bin/env python
# deletes a temp qcow

import os, sys, socket, logging
import sh
import argparse

from ceph_rsnapshot.logs import setup_logging()

temp_path = '/tmp/qcows'

def remove_qcow():
  parser = argparse.ArgumentParser(description='deletes a temporary qcow')
  parser.add_argument('image')
  # parser.add_argument('--sum', dest='accumulate', action='store_const',
  #                     const=sum, default=max,
  #                     help='sum the integers (default: find the max)')
  args = parser.parse_args()
  image = args.image

  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')

  logger = setup_logging()

  logger.info("deleting temp qcow for %s" % image)

  try:
    os.remove("%s/%s.qcow2" % (temp_path, image))
  except Exception as e:
    logger.error(e.stdout)
    logger.error(e.stderr)
    raise NameError, e
  logger.info("successfully removed qcow for %s" % image)
