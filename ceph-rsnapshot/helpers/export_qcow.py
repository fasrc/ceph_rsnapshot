#!/usr/bin/env python
# export a ceph image to qcow
# image is the image@snap name
# snap would be date --iso

# depends on qemu-img yum package

# TODO  print free space and size and etc

import os, sys, socket, logging
import sh
from sh import rbd
import argparse
import json
import time

temp_path = '/tmp/qcows'
min_freespace = 100*1024*1024 # 100mb

sh_logging = False

def setup_logging(log_location='/var/log/ceph-rsnapshot/', log_filename='export_qcow', verbose=False):
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


def setup_temp_path():
  try:
    dirlist = os.listdir(temp_path)
  except:
    os.mkdir(temp_path,0700)

def get_freespace(path):
  statvfs = os.statvfs(path)
  avail_bytes = statvfs.f_frsize * statvfs.f_bavail
  return avail_bytes

# TODO need to sum up sizes of snaps... may just use provisioned size instead
def get_rbd_size(image,cluster,snap=''):
  if snap == '':
    snap = get_today()
  rbd_image_string = "%s@%s" % (image, snap)
  # check the size of this image@snap
  rbd_du_result=rbd.du(rbd_image_string, cluster=cluster, format='json')
  rbd_image_used_size = json.loads(rbd_du_result.stdout)['images'][0]['used_size']
  rbd_image_provisioned_size = json.loads(rbd_du_result.stdout)['images'][0]['provisioned_size']
  return rbd_image_provisioned_size

def get_today():
  return sh.date('--iso').strip('\n')

def export_qcow(image,pool,cephuser,cluster,snap='',path=temp_path):
  if snap == '':
    snap = get_today()
  # use this because it has a dash in the command name
  qemuimg = sh.Command('qemu-img')
  # build up string arguments
  qemu_source_string = "rbd:%s/%s@%s:id=%s:conf=/etc/ceph/%s.conf" % (pool, image, snap, cephuser, cluster)
  qemu_dest_string = "%s/%s.qcow2" % (temp_path, image)
  # do the export
  try:
    ts=time.time()
    export_result = qemuimg.convert(qemu_source_string, qemu_dest_string, f='raw', O='qcow2')
    tf=time.time()
    elapsed_time = tf-ts
    elapsed_time_ms = elapsed_time * 10**3
  except Exception as e:
    logger.error("error exporting %s" % image)
    logger.error(e.stderr)
    raise NameError('error_exporting_qcow')
  return elapsed_time_ms

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Export a rbd image to qcow')
  parser.add_argument('image')
  # parser.add_argument('--sum', dest='accumulate', action='store_const',
  #                     const=sum, default=max,
  #                     help='sum the integers (default: find the max)')
  args = parser.parse_args()
  image = args.image

  logger = setup_logging()

  logger.info("exporting %s..." % image)

  # setup tmp path and check free space
  setup_temp_path()
  avail_bytes = get_freespace(temp_path)

  # check free space for this image snap
  rbd_image_used_size = get_rbd_size(image,cluster='ceph')
  logger.info("image size %s" % rbd_image_used_size)
  if rbd_image_used_size > ( avail_bytes - min_freespace ):
    raise NameError, "not enough free space to export this qcow"
    sys.exit(1)

  # export qcow
  try:
    elapsed_time_ms=export_qcow(image,pool='rbd',cephuser='admin',cluster='ceph')
    logger.info('image %s successfully exported in %sms' % (image, elapsed_time_ms))
  except Exception as e:
    logger.error(e)
    sys.exit(1)
