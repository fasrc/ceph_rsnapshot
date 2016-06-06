#!/usr/bin/env python
# gathers the pre-existing snaps to backup
# this script will live on the bu-mon01

import sh
from sh import rbd
import re
import argparse
import json
import os, sys, socket, logging

# helpers from ceph-rsnapshot common
from common import setup_logging


# images are one-NN
image_re = r'^one-[0-9]+$'
# vms are one-NN-XX-YY for image NN vm XX and disk YY
vm_re = r'^one(-[0-9]+){3}$'
# images or vms are (with the additional accidental acceptance of one-NN-XX
all_re = r'^one(-[0-9]+){1,3}$'


sh_logging = False


def list_pool(pool,filter=all_re):
  rbd_ls_result = rbd.ls(pool,format='json')
  if rbd_ls_result.exit_code != 0:
    raise NameError
  rbd_images_filtered = [image for image in json.loads(rbd_ls_result.stdout) if re.match(all_re,image)]
  return rbd_images_filtered

# FIXME need this on export qcow script too
def get_today():
  return sh.date('--iso').strip('\n')

# checks a image has a snap of given name,
# or of iso today format if no snap name passed
def check_snap(image,pool='rbd',snap=''):
  if snap == '':
    snap = get_today()
  # check if today snap exists for this image
  try:
    rbd_check_result = rbd.info('%s/%s@%s' % (pool, image, snap))
  except Exception as e:
    # for now just take any error and say it doesn't have a snap
    return False
    # raise NameError, e
    # if rbd_check_result.exit_code != 0:
    #   raise NameError, "today snap does not exist for image %s" % image
  return True

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Gather a list of rbd images in a given pool with snaps from today')
  parser.add_argument('pool', help='ceph pool to get list of rbd images for')
  # parser.add_argument('image')
  # parser.add_argument('--sum', dest='accumulate', action='store_const',
  #                     const=sum, default=max,
  #                     help='sum the integers (default: find the max)')
  args = parser.parse_args()
  pool = args.pool
  # print("exporting %s" % image)

  logger = setup_logging(log_filename='gathernames')

  images_to_check = list_pool(pool)
  images_with_snaps=[]
  images_without_snaps=[]
  for image in images_to_check:
    if check_snap(image):
      images_with_snaps.append(image)
    else:
      images_without_snaps.append(image)

  # FIXME todo wrap these to json
  logger.warning('these images had no snaps: %s' % ','.join(images_without_snaps)) #to stderr via stream handler above
  # print('these images have snaps: \n%s' % '\n'.join(images_with_snaps))

  # for now just print
  print('\n'.join(images_with_snaps)) # to stdout
  logger.info('images: %s' % ','.join(images_with_snaps)) # to stderr via stream handler above
