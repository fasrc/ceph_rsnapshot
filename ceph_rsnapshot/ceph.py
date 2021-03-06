# functions to work on ceph
import sh
import json
import time
import re

from ceph_rsnapshot import logs
from ceph_rsnapshot import dirs
from ceph_rsnapshot import settings
from ceph_rsnapshot import helpers
from ceph_rsnapshot import exceptions


def check_snap_status_file(cephhost='', snap_status_file_path=''):
    logger = logs.get_logger()
    if not cephhost:
        cephhost = settings.CEPH_HOST
    if not snap_status_file_path:
        snap_status_file_path = settings.SNAP_STATUS_FILE_PATH
    CHECK_SNAP_STATUS_DIR_COMMAND = ('ls -t %s/*' % snap_status_file_path)
    logger.info('checking snap status directory %s on ceph host'
             % snap_status_file_path)
    try:
        snap_status_dir_result = sh.ssh(cephhost, CHECK_SNAP_STATUS_DIR_COMMAND).strip('\n')
    except sh.ErrorReturnCode as e:
        if e.exit_code == 2:
            raise exceptions.NoSnapStatusFilesFoundError(cephhost=cephhost,
                    status_dir=snap_status_file_path, e=e)
        else:
            raise
    logger.debug("found: %s" % snap_status_dir_result)
    snap_dates = [ snap_status_file.split('/')[-1] for snap_status_file in snap_status_dir_result.split('\n')]
    logger.debug("snap dates:")
    logger.debug(snap_dates)
    snap_date = snap_dates[0]
    logger.debug("checking newest snap_date %s" % snap_date)
    try:
        result = check_formatted_snap_date(snap_date=snap_date)
    except exceptions.SnapDateNotValidDateError as e:
        raise
    # if we're here it was a valid date and matches format
    # remove the rest of them that match
    for old_snap_date in snap_dates[1:]:
        logger.warn("found old snap_date files- checking and removing if valid dates")
        try:
            logger.debug('checking old snap date %s' % old_snap_date)
            check_formatted_snap_date(snap_date=old_snap_date)
        except exceptions.SnapDateNotValidDateError as e:
            e.log(warn=True)
            continue
        except exceptions.SnapDateFormatMismatchError as e:
            e.log(warn=True)
            continue
        # if here then it's a valid date
        logger.warning('removing old snap_date status file %s because we have'
                ' a newer one %s' % (old_snap_date, snap_date))
        remove_snap_status_file(snap_date=old_snap_date)
    logger.info('using snap_date %s found from queue on ceph host' % snap_date)
    return snap_date


def check_formatted_snap_date(snap_date, snap_naming_date_format=''):
    if not snap_naming_date_format:
        snap_naming_date_format = settings.SNAP_NAMING_DATE_FORMAT
    formatted_snap_date = get_snapdate(snap_date=snap_date,
        snap_naming_date_format=snap_naming_date_format)
    if formatted_snap_date == snap_date:
        return True
    else:
        raise exceptions.SnapDateFormatMismatchError(snap_date=snap_date,
            date_format=snap_naming_date_format)

def remove_snap_status_file(snap_date, cephhost='', snap_status_file_path='',
        noop=''):
    logger = logs.get_logger()
    if not cephhost:
        cephhost = settings.CEPH_HOST
    if not snap_status_file_path:
        snap_status_file_path = settings.SNAP_STATUS_FILE_PATH
    if not noop:
        noop = settings.NOOP
    REMOVE_SNAP_STATUS_FILE_COMMAND = ('rm -fv %s/%s' % (snap_status_file_path,
            snap_date))
    logger.info('removing snap status file on ceph host with command %s' %
            REMOVE_SNAP_STATUS_FILE_COMMAND)
    if noop:
        logger.info('would have run %s' % REMOVE_SNAP_STATUS_FILE_COMMAND)
        remove_snap_status_file_result = 'noop'
    else:
        remove_snap_status_file_result = sh.ssh(cephhost, REMOVE_SNAP_STATUS_FILE_COMMAND).strip('\n')
    # TODO handle some errors gracefully here
    logger.info("done removing snap status file: %s" % remove_snap_status_file_result)
    return True


def check_snap(image, snap='', pool='', cephhost='', cephuser='', cephcluster='',
               snap_naming_date_format='', snap_date=''):
    """ ssh to ceph host and check for a snapshot
    """
    logger = logs.get_logger()
    if not snap_naming_date_format:
        snap_naming_date_format = settings.SNAP_NAMING_DATE_FORMAT
    if not snap_date:
        snap_date = settings.SNAP_DATE
    if not snap:
        snap = get_snapdate(snap_naming_date_format=snap_naming_date_format,
                            snap_date=snap_date)
    if not pool:
        pool = settings.POOL
    if not cephhost:
        cephhost = settings.CEPH_HOST
    if not cephuser:
        cephuser = settings.CEPH_USER
    if not cephcluster:
        cephcluster = settings.CEPH_CLUSTER
    RBD_CHECK_SNAP_COMMAND = ('rbd info %s/%s@%s --id=%s --cluster=%s' %
                              (pool, image, snap, cephuser, cephcluster))
    logger.info('checking for snap with command %s' % RBD_CHECK_SNAP_COMMAND)
    try:
        rbd_check_result = sh.ssh(cephhost, RBD_CHECK_SNAP_COMMAND)
    except sh.ErrorReturnCode as e:
        # this just means no snap found, log but don't raise
        logger.warning('no snap found for image %s' % image)
        # return false we didn't find one
        return False
    except Exception as e:
        logger.info('error getting list of images from ceph node')
        logger.exception(e)
        raise
    # if here then rbd snap found, so return True
    return True


def gathernames(pool='', cephhost='', cephuser='', cephcluster='',
                snap_naming_date_format='', image_re='', snap_date=''):
    """ ssh to ceph node and get list of rbd images that match snap naming
        format
    """
    logger = logs.get_logger()
    if not pool:
        pool = settings.POOL
    if not cephhost:
        cephhost = settings.CEPH_HOST
    if not cephuser:
        cephuser = settings.CEPH_USER
    if not cephcluster:
        cephcluster = settings.CEPH_CLUSTER
    if not snap_naming_date_format:
        snap_naming_date_format = settings.SNAP_NAMING_DATE_FORMAT
    if not snap_date:
        snap_date = settings.SNAP_DATE
    if not image_re:
        image_re = settings.IMAGE_RE
    RBD_LS_COMMAND = ('rbd ls %s --id=%s --cluster=%s --format=json' %
                      (pool, cephuser, cephcluster))
    logger.info('sshing to %s and running %s to get list of images' %
        (cephhost,RBD_LS_COMMAND))
    try:
        rbd_ls_result = sh.ssh(cephhost, RBD_LS_COMMAND)
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
                           re.match(image_re, image)]
    logger.info('images after filtering by image_re /%s/ are: %s' % (image_re,
        ' '.join(rbd_images_filtered)))
    # first sanitize names of images
    bad_names=[]
    for image in rbd_images_filtered:
        try:
            if helpers.validate_string(image):
                continue
        except NameError as e:
            # bad character in a string, don't use this image
            logger.warning('disallowed character in image name: %s'
                ' error %s' % (image, e))
            # take it out of the good array
            rbd_images_filtered.remove(image)
            bad_names.append(image)
        except Exception as e:
            raise
    # now check for snaps
    images_with_snaps = []
    images_without_snaps = []
    for image in rbd_images_filtered:
        if check_snap(image):
            images_with_snaps.append(image)
        else:
            images_without_snaps.append(image)
    logger.info('images with snaps are: %s' % ' '.join(images_with_snaps))
    if images_without_snaps:
        logger.warning('note, found %s images with no snaps' %
                       len(images_without_snaps))
    if bad_names:
        logger.warning('note, found %s images with bad names' %
                       len(bad_names))
    return images_with_snaps


def get_freespace(path=''):
    """ssh to ceph node and get freespace for a path
       if not specified, use settings.QCOW_TEMP_PATH/settings.POOL
    """
    logger = logs.get_logger()
    if not path:
        path = "%s/%s" % (settings.QCOW_TEMP_PATH, settings.POOL)
    DF_COMMAND = ("LANG='' LC_CTYPE='' df -P %s | grep / | awk '{print $4}';"
                  " ( exit ${PIPESTATUS[0]} )" % path)
    try:
        free_space_kb = sh.ssh(settings.CEPH_HOST, DF_COMMAND)
        free_space_bytes = int(free_space_kb.stdout) * 1024
        return free_space_bytes
    except sh.ErrorReturnCode as e:
        logger.error('error getting free space on ceph node for %s' % path)
        logger.exception(e.stderr)
        # pass it up
        raise
    except Exception as e:
        logger.error('error getting free space on ceph node')
        logger.exception(e)
        # pass it up
        raise


def get_snapdate(snap_naming_date_format='', snap_date=''):
    """get todays date in iso format, this can run on either node
    """
    logger=logs.get_logger()
    if not snap_naming_date_format:
        snap_naming_date_format = settings.SNAP_NAMING_DATE_FORMAT
    if not snap_date:
        snap_date = settings.SNAP_DATE
    try:
        converted_snap_date = sh.date('+%s' % snap_naming_date_format,
                   date=snap_date).strip('\n')
    except sh.ErrorReturnCode as e:
        if e.exit_code == 1:
            raise(exceptions.SnapDateNotValidDateError(snap_date=snap_date,
                date_format=snap_naming_date_format, e=e))
        else:
            raise
    return converted_snap_date


def get_rbd_size(image, snap='', pool='', cephhost='', cephuser='',
        cephcluster='', snap_naming_date_format='', snap_date=''):
    """ssh to ceph node check the size of this image@snap
    """
    logger = logs.get_logger()
    if not snap_naming_date_format:
        snap_naming_date_format = settings.SNAP_NAMING_DATE_FORMAT
    if not snap_date:
        snap_date = settings.SNAP_DATE
    if not snap:
        snap = get_snapdate(snap_naming_date_format=snap_naming_date_format,
                            snap_date=snap_date)
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


def export_qcow(image, snap='', pool='', cephhost='', cephuser='', cephcluster='',
                noop=None, snap_naming_date_format='', snap_date=''):
    """ssh to ceph node, check free space vs rbd provisioned size, 
        and export a qcow to qcow_temp_path/pool/imagename.qcow2
    """
    logger = logs.get_logger()
    if not snap_naming_date_format:
        snap_naming_date_format = settings.SNAP_NAMING_DATE_FORMAT
    if not snap_date:
        snap_date = settings.SNAP_DATE
    if not snap:
        snap = get_snapdate(snap_naming_date_format=snap_naming_date_format,
                            snap_date=snap_date)
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
    logger.info('going to export image %s@%s from pool %s on ceph host %s cluster %s'
                ' as user %s' % (image, snap, pool, cephhost, cephcluster,
                cephuser))

    # if any of these errors, fail this export and raise the errors up
    avail_bytes = get_freespace(settings.QCOW_TEMP_PATH)
    rbd_image_used_size = get_rbd_size(image)

    logger.info("image size %s" % rbd_image_used_size)
    if rbd_image_used_size > (avail_bytes - settings.MIN_FREESPACE):
        logger.error("not enough free space to export this qcow")
        raise NameError('not enough space to export this qcow')

    # build source and dest strings
    qemu_source_string = ("rbd:%s/%s@%s:id=%s:conf=/etc/ceph/%s.conf" %
                          (pool, image, snap, cephuser, cephcluster))
    qemu_dest_string = "%s/%s/%s@%s.qcow2" % (
        settings.QCOW_TEMP_PATH, pool, image, snap)
    # do the export
    QEMU_IMG_COMMAND = ('qemu-img convert %s %s -f raw'
                        ' -O qcow2' % (qemu_source_string, qemu_dest_string))
    logger.info('running rbd export on ceph host %s with command %s' %
                (cephhost, QEMU_IMG_COMMAND))
    try:
        ts = time.time()
        if noop:
            logger.info('NOOP: would have exported qcow')
        else:
            export_result = sh.ssh(cephhost, QEMU_IMG_COMMAND)
        tf = time.time()
        elapsed_time = tf - ts
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
    logger.info('qcow exported in %s ms' % elapsed_time_ms)
    return elapsed_time_ms


def remove_qcow(image, pool='', cephhost='', cephuser='', cephcluster='',
                snap_naming_date_format='', snap_date='', snap='', noop=None):
    """ ssh to ceph node and remove a qcow from path
        qcow_temp_path/pool/imagename.qcow2
    """
    logger = logs.get_logger()
    if not snap_naming_date_format:
        snap_naming_date_format = settings.SNAP_NAMING_DATE_FORMAT
    if not snap_date:
        snap_date = settings.SNAP_DATE
    if not snap:
        snap = get_snapdate(snap_naming_date_format=snap_naming_date_format,
                            snap_date=snap_date)
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
    temp_qcow_file = ("%s/%s/%s@%s.qcow2" % (settings.QCOW_TEMP_PATH,
                                          settings.POOL, image, snap))
    logger.info("deleting temp qcow from path %s on ceph host %s" %
                (temp_qcow_file, cephhost))
    SSH_RM_QCOW_COMMAND = 'rm %s' % temp_qcow_file
    try:
        if settings.NOOP:
            logger.info('NOOP: would have removed temp qcow for image %s from'
                        ' ceph host %s with command %s' % (image, cephhost,
                                                           SSH_RM_QCOW_COMMAND))
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
    return True
