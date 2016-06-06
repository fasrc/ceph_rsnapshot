#!/usr/bin/env python
# gathers the pre-existing snaps to backup
# this script will live on the bu-mon01

import sh
from sh import rbd
import re
import argparse
import json
import os, sys, socket, logging

# images are one-NN
image_re = r'^one-[0-9]+$'
# vms are one-NN-XX-YY for image NN vm XX and disk YY
vm_re = r'^one(-[0-9]+){3}$'
# images or vms are (with the additional accidental acceptance of one-NN-XX
all_re = r'^one(-[0-9]+){1,3}$'


sh_logging = False


def setup_logging(log_location='/var/log/ceph-rsnapshot/', log_filename='gathernames', verbose=False):
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
  consoleHandler = logging.StreamHandler(stream=sys.stderr)
  consoleHandler.setFormatter(logFormatter)
  logger.addHandler(consoleHandler)
  if sh_logging:
    sh_logger.addHandler(consoleHandler)
  return(logger)


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

  logger = setup_logging()

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
