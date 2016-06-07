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

from ceph_rsnapshot.logs import setup_logging
from ceph_rsnapshot import logs
from ceph_rsnapshot import settings

def setup_temp_path(pool):
  logger = logs.get_logger()
  temp_path = settings.QCOW_TEMP_PATH
  if not os.path.isdir(temp_path):
    os.mkdir("%s/%s" % (temp_path,pool),0700)
  if not os.path.isdir("%s/%s" % (temp_path,pool)):
    os.mkdir("%s/%s" % (temp_path,pool),0700)

def get_freespace(path):
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  statvfs = os.statvfs(path)
  avail_bytes = statvfs.f_frsize * statvfs.f_bavail
  return avail_bytes

# TODO need to sum up sizes of snaps... may just use provisioned size instead
def get_rbd_size(image,pool='rbd',cephuser='admin',cephcluster='ceph',snap=''):
  if snap == '':
    snap = get_today()
  rbd_image_string = "%s/%s@%s" % (pool, image, snap)
  # check the size of this image@snap
  rbd_du_result=rbd.du(rbd_image_string, user=cephuser, cluster=cephcluster, format='json')
  rbd_image_used_size = json.loads(rbd_du_result.stdout)['images'][0]['used_size']
  rbd_image_provisioned_size = json.loads(rbd_du_result.stdout)['images'][0]['provisioned_size']
  return rbd_image_provisioned_size

def get_today():
  return sh.date('--iso').strip('\n')

def export_qcow_sh(image,pool,cephuser,cephcluster,snap=''):
  # get logger we setup earlier
  logger = logging.getLogger('ceph_rsnapshot')
  if snap == '':
    snap = get_today()
  # use this because it has a dash in the command name
  qemuimg = sh.Command('qemu-img')
  # build up string arguments
  qemu_source_string = "rbd:%s/%s@%s:id=%s:conf=/etc/ceph/%s.conf" % (pool, image, snap, cephuser, cephcluster)
  qemu_dest_string = "%s/%s/%s.qcow2" % (settings.QCOW_TEMP_PATH, pool, image)
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

def export_qcow():
  parser = argparse.ArgumentParser(description='Export a rbd image to qcow')
  parser.add_argument('image')
  parser.add_argument('--pool', default='rbd')
  parser.add_argument('--cephuser', default='admin')
  parser.add_argument('--cephcluster', default='ceph')
  # parser.add_argument('--sum', dest='accumulate', action='store_const',
  #                     const=sum, default=max,
  #                     help='sum the integers (default: find the max)')
  args = parser.parse_args()
  image = args.image
  pool = args.pool
  cephuser = args.cephuser
  cephcluster = args.cephcluster

  settings.load_settings()
  settings.POOL = pool
  settings.CEPH_USER = cephuser
  settings.CEPH_CLUSTER = cephcluster

  logger = setup_logging()

  logger.info("exporting image %s from pool %s..." % (image,pool))

  # setup tmp path and check free space
  setup_temp_path(pool)
  avail_bytes = get_freespace(settings.QCOW_TEMP_PATH)

  # check free space for this image snap
  rbd_image_used_size = get_rbd_size(image,pool=pool,cephcluster='ceph')
  logger.info("image size %s" % rbd_image_used_size)
  if rbd_image_used_size > ( avail_bytes - settings.MIN_FREESPACE ):
    raise NameError, "not enough free space to export this qcow"
    sys.exit(1)

  # export qcow
  try:
    elapsed_time_ms=export_qcow_sh(image,pool=pool,cephuser=cephuser,cephcluster=cephcluster)
    logger.info('image %s successfully exported in %sms' % (image, elapsed_time_ms))
  except Exception as e:
    logger.error('error exporting image %s to qcow with error %s' % (image, e))
    sys.exit(1)
