# functions to work on ceph
import sh
import json
import time
import re

from ceph_rsnapshot import logs
from ceph_rsnapshot import dirs
from ceph_rsnapshot import settings


def check_snap(image,snap='',pool='',cephhost='',cephuser='',cephcluster=''):
    """ ssh to ceph host and check for a snapshot
    """
    logger = logs.get_logger()
    # TODO use snap name format
    if not snapnamingformat:
        snapnamingformat = settings.SNAP_NAMING_DATE_FORMAT
    if not snap:
        snap = get_today()
    if not pool:
        pool = settings.POOL
    if not cephhost:
        cephhost = settings.CEPH_HOST
    if not cephuser:
        cephuser = settings.CEPH_USER
    if not cephcluster:
        cephcluster = settings.CEPH_CLUSTER
    #TODO use user
    RBD_CHECK_SNAP_COMMAND = ('rbd info %s/%s@%s --cluster %s' % 
        (pool, image, snap, cephcluster))
    logger.info('checking for snap with command %s' % RBD_CHECK_SNAP_COMMAND)
    try:
        rbd_check_result = sh.ssh(cephhost, RBD_CHECK_SNAP_COMMAND)
    except sh.ErrorReturnCode as e:
        # this just means no snap found, log but don't raise
        logger.info('no snap found for image %s' % image)
        # return false we didn't find one
        return False
    except Exception as e:
        logger.info('error getting list of images from ceph node')
        logger.exception(e)
        raise
    # if here then rbd snap found, so return True
    return True


def gathernames(pool='',cephhost='',cephuser='',cephcluster='',
        snapnamingformat='',image_re=''):
    """ ssh to ceph node and get list of rbd images that match snap naming
        format
    """
    logger = logs.get_logger()
    if not snap:
        snap = get_today()
    if not pool:
        pool = settings.POOL
    if not cephhost:
        cephhost = settings.CEPH_HOST
    if not cephuser:
        cephuser = settings.CEPH_USER
    if not cephcluster:
        cephcluster = settings.CEPH_CLUSTER
    if not snapnamingformat:
        snapnamingformat = settings.SNAP_NAMING_DATE_FORMAT
    if not image_re:
        imagere = settings.IMAGE_RE
    logger.info('getting list of images in pool %s that match image_re %s'
        ' from ceph host %s cluster %s that have snaps matching date'
        ' format %s' % (pool, imagere, cephhost, cephcluster, snapnamingformat))
    # TODO use rbd user
    RBD_LS_COMMAND = ('rbd ls %s --cluster=%s --format=json' %
        (pool, cephcluster))
    try:
        rbd_ls_result = sh.ssh(cephhost,RBD_LS_COMMAND)
    except sh.ErrorReturnCode as e:
        logger.info('error getting list of images from ceph node')
        logger.exception(e.stderr)
        raise
    except Exception as e:
        logger.info('error getting list of images from ceph node')
        logger.exception(e)
        raise
    rbd_images_unfiltered = json.loads(rbd_ls_result.stdout)
    logger.info('all images: %s' % ' '.join(rbd_images_unfiltered))
    # filter by image_re
    rbd_images_filtered = [image for image in rbd_images_unfiltered if 
        re.match(image_re,image)]
    logger.info('images after filtering by image_re "%s" are:' % (image_re,
        ' '.join(rbd_images_filtered)))
    # now check for snaps
    images_with_snaps=[]
    images_without_snaps=[]
    for image in images_to_check:
      if check_snap(image):
        images_with_snaps.append(image)
      else:
        images_without_snaps.append(image)
    if images_without_snaps:
        logger.warning('found %s images with no snaps' % 
            len(images_without_snaps))
    return images_with_snaps


def get_freespace(path=''):
    """ssh to ceph node and get freespace for a path
       if not specified, use settings.QCOW_TEMP_PATH/settings.POOL
    """
    logger = logs.get_logger()
    if not path:
        path = "%s/%s" % (settings.QCOW_TEMP_PATH, settings.POOL)
    DF_COMMAND = "df -P %s | grep / | awk '{print $4}'" % path
    try:
        free_space_kb = sh.ssh(settings.CEPH_HOST, DF_COMMAND)
        return free_space * 1024
    except sh.ErrorReturnCode as e:
        # TODO handle ssh error
        logger.error('error getting free space on ceph node for %s' % path)
        logger.exception(e.stderr)
        # pass it up
        raise
    except Exception as e:
        # TODO handle other error
        logger.error('error getting free space on ceph node')
        logger.exception(e)
        # pass it up
        raise


def get_today():
    """get todays date in iso format, this can run on either node
    """
    # TODO use settings.IMAGE_RE
    return sh.date('--iso').strip('\n')


def get_rbd_size(image,snap='',pool='',cephhost='',cephuser='',cephcluster=''):
    """ssh to ceph node check the size of this image@snap
    """
    logger = logs.get_logger()
    if not snap:
        snap = get_today()
    if not pool:
        pool = settings.POOL
    if not cephhost:
        cephhost = settings.CEPH_HOST
    if not cephuser:
        cephuser = settings.CEPH_USER
    if not cephcluster:
        cephcluster = settings.CEPH_CLUSTER
    rbd_image_string = "%s/%s@%s" % (pool, image, snap)
    RBD_COMMAND = ('rbd du %s --user=%s --cluster=%s --format=json' %
            (rbd_image_string, cephuser, cephcluster))
    logger.info('getting rbd size from ceph host %s with command %s' % 
        (cephhost, RBD_COMMAND))
    try:
        rbd_du_result = sh.ssh(cephhost, RBD_COMMAND)
        rbd_image_used_size = (json.loads(rbd_du_result.stdout)['images'][0]
            ['used_size'])
        rbd_image_provisioned_size = (json.loads(rbd_du_result.stdout)['images']
            [0]['provisioned_size'])
        # using provisioned_size as these are snaps and space could be on the
        # parent
        return rbd_image_provisioned_size
    except sh.ErrorReturnCode as e:
        logger.error('error getting rbd size for %s, output from ssh:' %
            rbd_image_string)
        logger.exception(e.stderr)
        raise
    except Exception as e:
        logger.error('error getting rbd size for %s' % rbd_image_string)
        logger.exception(e)
        raise


def export_qcow(image,snap='',pool='',cephhost='',cephuser='',cephcluster='',
        noop=None):
    """ssh to ceph node, check free space vs rbd provisioned size, 
        and export a qcow to qcow_temp_path/pool/imagename.qcow2
    """
    logger = logs.get_logger()
    if not snap:
        snap = get_today()
    if not pool:
        pool = settings.POOL
    if not cephhost:
        cephhost = settings.CEPH_HOST
    if not cephuser:
        cephuser = settings.CEPH_USER
    if not cephcluster:
        cephcluster = settings.CEPH_CLUSTER
    if not noop:
        noop = settings.NOOP
    logger.info('exporting image %s@%s from pool %s on ceph host %s cluster %s'
        ' as user %s' % (image, snap, pool, cephhost, cephuser, cephcluster))

    # if any of these errors, fail this export and raise the errors up
    dirs.setup_qcow_temp_path(settings.POOL)
    avail_bytes = get_freespace(settings.QCOW_TEMP_PATH)
    rbd_image_used_size = get_rbd_size(image)

    logger.info("image size %s" % rbd_image_used_size)
    if rbd_image_used_size > ( avail_bytes - settings.MIN_FREESPACE ):
        logger.error("not enough free space to export this qcow")
        raise NameError('not enough space to export this qcow')

    # build source and dest strings
    qemu_source_string = "rbd:%s/%s@%s:id=%s:conf=/etc/ceph/%s.conf" %
        (pool, image, snap, cephuser, cephcluster)
    qemu_dest_string = "%s/%s/%s.qcow2" % (settings.QCOW_TEMP_PATH, pool, image)
    # do the export
    QEMU_IMG_COMMAND = ('qemu-img convert %s %s f=raw'
        ' O=qcow2' % (qemu_source_string, qemu_dest_string))
    logger.info('running rbd export on ceph host %s with command %s' % 
        (cephhost, QEMU_IMG_COMMAND))
    try:
        ts=time.time()
        if noop:
            logger.info('NOOP: would have exported qcow')
        else:
            export_result = sh.ssh(cephhost, QEMU_IMG_COMMAND)
        tf=time.time()
        elapsed_time = tf-ts
        elapsed_time_ms = elapsed_time * 10**3
    except sh.ErrorReturnCode as e:
        logger.error('error exporting qcow with command %s on ceph host %s,'
            ' output from ssh:' % (cephhost, QEMU_IMG_COMMAND))
        logger.exception(e.stderr)
        raise
    except Exception as e:
        logger.error('error exporting qcow with command %s on ceph host %s,'
            ' output from ssh:' % (cephhost, QEMU_IMG_COMMAND))
        logger.exception(e)
        raise
    return elapsed_time_ms


def remove_qcow(image,snap='',pool='',cephhost='',cephuser='',cephcluster='',
        noop=None):
    """ ssh to ceph node and remove a qcow from path
        qcow_temp_path/pool/imagename.qcow2
    """
    logger = logs.get_logger()
    if not snap:
        snap = get_today()
    if not pool:
        pool = settings.POOL
    if not cephhost:
        cephhost = settings.CEPH_HOST
    if not cephuser:
        cephuser = settings.CEPH_USER
    # TODO use ceph cluster in path naming
    if not cephcluster:
        cephcluster = settings.CEPH_CLUSTER
    if not noop:
        noop = settings.NOOP
    temp_qcow_file = ("%s/%s/%s.qcow2" % (settings.QCOW_TEMP_PATH,
        settings.POOL, image))
    logger.info("deleting temp qcow from path %s on ceph host %s" % 
        (image, settings.POOL))
    SSH_RM_QCOW_COMMAND = 'rm %s' % temp_qcow_file
    try:
        if settings.NOOP:
            logger.info('NOOP: would have removed temp qcow for image %s from'
                ' ceph host %s with command %s' % (image, cephhost,
                SSH_RM_QCOW_COMAMND))
        else:
            sh.ssh(cephhost, SSH_RM_QCOW_COMMAND)
    except sh.ErrorReturnCode as e:
        logger.error('error removing temp qcow %s with error from ssh:' 
            % temp_qcow_file)
        logger.exception(e.stderr)
        raise
    except Exception as e:
        logger.error('error removing temp qcow %s' 
            % temp_qcow_file)
        logger.exception(e)
        raise
    logger.info("successfully removed qcow for %s" % image)
