#!/usr/bin/env python
# gathers the pre-existing snaps to backup
# this script will live on the bu-mon01

import sh
from sh import rbd
import re
import argparse
import json
import os, sys, socket, logging

from ceph_rsnapshot import logs
from ceph_rsnapshot import settings


def list_pool(pool,image_re=''):
  # can't log to stdout until TODO pass json back to rsnapshot node 
  # so don't log if noop - if noop can't write to file either
  logger = logs.get_logger()
  if not image_re:
    image_re = settings.IMAGE_RE
  try:
    rbd_ls_result = rbd.ls(pool,cluster=settings.CEPH_CLUSTER,format='json')
  except Exception as e:
    logger.error(e)
    raise NameError
  rbd_images_unfiltered = json.loads(rbd_ls_result.stdout)
  logger.info('all images: %s' % ' '.join(rbd_images_unfiltered))
  rbd_images_filtered = [image for image in rbd_images_unfiltered if re.match(image_re,image)]
  logger.info('images after filtering by image_re: %s' % ' '.join(rbd_images_filtered))
  return rbd_images_filtered

# FIXME need this on export qcow script too
# FIXME use SNAP_NAMING_DATE_FORMAT
def get_today():
  return sh.date('--iso').strip('\n')

# checks a image has a snap of given name,
# or of iso today format if no snap name passed
def check_snap(image,pool='',snap=''):
  # can't log to stdout until TODO pass json back to rsnapshot node 
  # so don't log if noop - if noop can't write to file either
  if not settings.NOOP:
    logger = logs.get_logger()
  if not pool:
    pool=settings.POOL
  if not snap:
    snap = get_today()
  # check if today snap exists for this image
  try:
    rbd_check_result = rbd.info('%s/%s@%s' % (pool, image, snap),cluster=settings.CEPH_CLUSTER)
    logger.info('found snap for image %s/%s' % (pool, image))
  except Exception as e:
    logger.warning('no snap found for image %s/%s' % (pool, image))
    # for now just take any error and say it doesn't have a snap
    return False
  return True

def gathernames():
  parser = argparse.ArgumentParser(description='Gather a list of rbd images in a given pool with snaps from today',
                                   argument_default=argparse.SUPPRESS)
  parser.add_argument('--pool', required=False, help='ceph pool to get list of rbd images for')
  parser.add_argument("--noop", action='store_true',required=False, help="noop - don't make any directories or do any actions. logging only to stdout")
  # parser.add_argument('image')
  # parser.add_argument('--sum', dest='accumulate', action='store_const',
  #                     const=sum, default=max,
  #                     help='sum the integers (default: find the max)')
  args = parser.parse_args()

  settings.load_settings()

  if args.__contains__('pool'):
    settings.POOL = args.pool
  if args.__contains__('noop'):
    settings.NOOP = args.noop

  # can't log to stdout until TODO pass json back to rsnapshot node 
  # so don't log if noop - if noop can't write to file either
  if not settings.NOOP:
    logger = logs.setup_logging(stdout=False)

  logger.info('gathernames starting checking for images in pool %s on cluster %s' %(settings.POOL, settings.CEPH_CLUSTER))
  images_to_check = list_pool(settings.POOL)
  images_with_snaps=[]
  images_without_snaps=[]
  for image in images_to_check:
    if check_snap(image):
      images_with_snaps.append(image)
    else:
      images_without_snaps.append(image)

  # FIXME todo wrap these to json
  # logger.warning('these images had no snaps: %s' % ','.join(images_without_snaps)) #to stderr via stream handler above
  # print('these images have snaps: \n%s' % '\n'.join(images_with_snaps))

  # for now just print
  print('\n'.join(images_with_snaps)) # to stdout
  # logger.info('images: %s' % ','.join(images_with_snaps)) # to stderr via stream handler above
  logger.info('gathernames done')
