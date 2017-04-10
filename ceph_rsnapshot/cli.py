#!/usr/bin/env python
import sh
import os
import sys
import argparse
import time
import re
import logging
import json

from ceph_rsnapshot import logs
from ceph_rsnapshot import settings
from ceph_rsnapshot import templates
from ceph_rsnapshot import dirs
from ceph_rsnapshot import ceph
from ceph_rsnapshot import helpers
from ceph_rsnapshot import exceptions


# TODO FIXME add a timeout on the first ssh connection and error
# differently if the source is not responding


def get_names_on_dest(pool=''):
    logger = logs.get_logger()
    if not pool:
        pool = settings.POOL
    backup_base_path = settings.BACKUP_BASE_PATH
    # get logger we setup earlier
    logger = logging.getLogger('ceph_rsnapshot')
    backup_path = "%s/%s" % (backup_base_path, pool)
    try:
        names_on_dest = os.listdir(backup_path)
    except (IOError, OSError) as e:
        if settings.NOOP:
            # this will fail if noop and the dir doesn't exist, so
            # fake nothing there and move on
            logger.info('NOOP: would have listed vms in directory %s' %
                        backup_path)
            return []
        logger.error(e)
        raise NameError('get_names_on_dest failed with error %s' % e)
    # FIXME cehck error
    return names_on_dest


def rotate_orphans(orphans, pool=''):
    logger = logs.get_logger()
    if not pool:
        pool = settings.POOL
    backup_base_path = settings.BACKUP_BASE_PATH

    # now check for ophans on dest
    backup_path = "%s/%s" % (backup_base_path, pool)

    orphans_rotated = []
    orphans_failed_to_rotate = []

    template = templates.get_template()

    empty_tempdir = dirs.make_empty_tempdir()

    for orphan in orphans:
        logger.info('rotating orphan: %s' % orphan)
        try:
            # do this every time to be sure it's empty
            dirs.check_empty_dir(empty_tempdir)
        except NameError as e:
            logger.error('error with verifying temp empty source,'
                         ' cannot rotate orphans. error: %s' % e)
            # fail out
            return({'orphans_rotated': orphans_rotated, 'orphans_failed_to_rotate':
                    [orphan for orphan in orphans if orphan not in orphans_rotated]})
        # note this uses temp_path on the dest - which we check to be empty
        # note needs to end in a trailing /
        source = "%s/" % empty_tempdir
        conf_file = templates.write_conf(orphan,
                                         pool=pool,
                                         source=source,
                                         template=template)
        logger.info("rotating orphan %s" % orphan)
        if settings.NOOP:
            logger.info(
                'NOOP: would have rotated orphan here using rsnapshot conf see previous lines')
        else:
            try:
                rsnap_result = sh.rsnapshot(
                    '-c', '%s/%s/%s.conf' % (settings.TEMP_CONF_DIR, pool, orphan), settings.RETAIN_INTERVAL)
                # if ssuccessful, log
                if rsnap_result.stdout.strip("\n"):
                    logger.info("successful; stdout from rsnap:\n" +
                                rsnap_result.stdout.strip("\n"))
                orphans_rotated.append({'pool': pool, 'orphan': orphan})
            except sh.ErrorReturnCode as e:
                orphans_failed_to_rotate.append({'pool': pool, 'orphan': orphan})
                logger.error("failed to rotate orphan %s with code %s" %
                             (orphan, e.exit_code))
                logger.error("stdout from source node:\n" +
                             e.stdout.strip("\n"))
                logger.error("stderr from source node:\n" +
                             e.stderr.strip("\n"))
        # unless flag to keep it for debug
        if not settings.KEEPCONF:
            templates.remove_conf(orphan, pool)

    dirs.remove_empty_dir(empty_tempdir)

    # TODO now check for any image dirs that are entirely empty and remove
    # them (and the empty daily.NN inside them)
    return({'orphans_rotated': orphans_rotated, 'orphans_failed_to_rotate': orphans_failed_to_rotate})


def rsnap_image_sh(image, pool=''):
    logger = logs.get_logger()
    if not pool:
        pool = settings.POOL
    # TODO check free space before rsnapping
    logger.info("rsnapping %s" % image)
    rsnap_conf_file = '%s/%s/%s.conf' % (settings.TEMP_CONF_DIR, pool, image)
    if settings.NOOP:
        logger.info('NOOP: would have rsnapshotted image from conf file '
                    '%s/%s/%s.conf for retain interval %s ' % (settings.TEMP_CONF_DIR,
                                                               pool, image, settings.RETAIN_INTERVAL))
        # set this False so it's clear this wasn't successful as it was a noop
        rsnap_ok = False
    else:
        try:
            ts = time.time()
            rsnap_result = sh.rsnapshot(
                '-c', rsnap_conf_file, settings.RETAIN_INTERVAL)
            tf = time.time()
            elapsed_time = tf - ts
            elapsed_time_ms = elapsed_time * 10**3
            rsnap_ok = True
            logger.info("rsnap successful for image %s in %sms" %
                        (image, elapsed_time_ms))
            if rsnap_result.stdout.strip("\n"):
                logger.info("stdout from rsnap:\n" +
                            rsnap_result.stdout.strip("\n"))
        except sh.ErrorReturnCode as e:
            logger.error("failed to rsnap %s with code %s" %
                         (image, e.exit_code))
            # TODO move log formatting and writing to a function
            logger.error("stdout from rsnap:\n" + e.stdout.strip("\n"))
            logger.error("stderr from rsnap:\n" + e.stderr.strip("\n"))
            rsnap_ok = False
    return rsnap_ok


def rsnap_image(image, pool='', template=None):
    if not pool:
        pool = settings.POOL
    temp_path = settings.QCOW_TEMP_PATH
    extra_args = settings.EXTRA_ARGS

    conf_base_path = settings.TEMP_CONF_DIR
    backup_base_path = settings.BACKUP_BASE_PATH

    # get logger we setup earlier
    logger = logs.get_logger()
    logger.info('working on image %s in pool %s' % (image, pool))
    # setup flags
    qcow_temp_path_empty = False
    export_qcow_ok = False
    rsnap_ok = False
    remove_qcow_ok = False

    # only reopen if we haven't pulled this yet - ie, are we part of a pool run
    if not template:
        template = templates.get_template()

    # create the temp conf file
    conf_file = templates.write_conf(image, pool=pool, template=template)
    logger.info(conf_file)

    # make sure temp qcow dir is empty
    try:
        if dirs.check_qcow_temp_path_empty_for_pool(pool=pool):
            qcow_temp_path_empty = True
    except Exception as e:
        # if it's not empty, fail this image
        logger.error('qcow temp path not empty, failing this image')
        logger.exception(e)
        qcow_temp_path_empty = False

    # ssh to source and export temp qcow of this image
    if qcow_temp_path_empty:
        try:
            ceph.export_qcow(image, pool=pool)
            export_qcow_ok = True
        except NameError as e:
            # probably not enough space. set to false and try to go and remove this
            # one, or go to next image in case it was temporary
            logger.error('error from export qcow: %s' % e)
            export_qcow_ok = False
        except Exception as e:
            logger.error('error from export qcow')
            logger.exception(e)
            export_qcow_ok = False

    # if exported ok, then rsnap this image
    if export_qcow_ok:
        try:
            rsnap_ok = rsnap_image_sh(image, pool=pool)
        except Exception as e:
            # TODO
            logger.error('error with rsnapping image %s' % image)
    else:
        logger.error(
            "skipping rsnap of image %s because export to qcow failed" % image)

    # either way remove the temp qcow
    logger.info("removing temp qcow for %s" % image)
    try:
        remove_qcow_ok = ceph.remove_qcow(image, pool=pool)
    except Exception as e:
        logger.error('error removing qcow. will continue to next image anyways,'
                     ' note that we check for free space so wont entirely fill disk if they'
                     ' all fail')

    # either way remove the temp conf file
    # unless flag to keep it for debug
    if not settings.KEEPCONF:
        templates.remove_conf(image, pool=pool)

    if export_qcow_ok and rsnap_ok and remove_qcow_ok:
        successful = True
    else:
        successful = False
    # return a blob with the details
    return({'image': image,
            'pool': pool,
            'successful': successful,
            'status': {
                'qcow_temp_path_empty': qcow_temp_path_empty,
                'export_qcow_ok': export_qcow_ok,
                'rsnap_ok': rsnap_ok,
                'remove_qcow_ok': remove_qcow_ok
            }
            })


def rsnap_pool(pool):
    # get values from settings
    host = settings.CEPH_HOST

    # get logger we setup earlier
    logger = logs.get_logger()
    logger.debug("starting rsnap of ceph pool %s to qcows in %s/%s" %
                 (pool, settings.BACKUP_BASE_PATH, pool))

    # get list of images from source
    try:
        names_on_source = ceph.gathernames(pool=pool)
    except Exception as e:
        logger.error('cannot get names from source with error %s' % e)
        # fail out
        raise NameError('cannot get names on source, failing run')
    logger.info("names on source: %s" % ",".join(names_on_source))

    # get list of images on backup dest already
    try:
        names_on_dest = get_names_on_dest(pool=pool)
    except Exception as e:
        logger.error('cannot get names from dest with error %s' % e)
        # fail out
        raise NameError('cannot get names on dest, failing run')
    logger.info("names on dest: %s" % ",".join(names_on_dest))

    # calculate difference
    orphans_on_dest = [
        image for image in names_on_dest if image not in names_on_source]
    if orphans_on_dest:
        logger.info("orphans on dest: %s" % ",".join(orphans_on_dest))

    # get template string for rsnap conf
    template = templates.get_template()

    successful = []
    failed = []
    orphans_rotated = []
    orphans_failed_to_rotate = []

    len_names = len(names_on_source)
    index = 1
    if len_names == 1 and names_on_source[0] == u'':
        # TODO decide if this is critical/stop or just warn
        logger.critical('no images found on source')
        # sys.exit(1)
    else:
        for image in names_on_source:
            # just to be safe, sanitize image names here too
            try:
                helpers.validate_string(image)
            except NameError as e:
                logger.error('bad character in image name %s: error %s' % 
                    (image, e))
                # fake return value from image
                failed.append({'image': image,
                    'pool': pool,
                    'successful': False,
                    'status': {
                        'export_qcow_ok': False,
                        'rsnap_ok': False,
                        'remove_qcow_ok': False
                    }
                })
                continue
            # TODO catch other exceptions here?
            logger.info('working on name %s of %s in pool %s: %s' %
                        (index, len_names, pool, image))

            try:
                result = rsnap_image(image, pool=pool, template=template)
                if result['successful']:
                    logger.info('successfully done with %s' % image)
                    # store in array
                    successful.append(result)
                else:
                    logger.error('error on %s : result: %s' %
                                (image, result['status']))
                    # store dict in dict
                    failed.append(result)
            except Exception as e:
                logger.error('error with pool %s at image %s' % (pool, image))
                logger.exception(e)
            # done with this image, increment counter
            index = index + 1

    # {'orphans_rotated': orphans_rotated, 'orphans_failed_to_rotate': orphans_failed_to_rotate}
    if settings.NO_ROTATE_ORPHANS:
      logger.info('not rotating orphans')
      orphan_result=dict(orphans_rotated=['no_rotate_orphans was set True'],
        orphans_failed_to_rotate=['no_rotate_orphans was set True'])
    else:
        try:
            orphan_result = rotate_orphans(orphans_on_dest, pool=pool)
        except Exception as e:
            logger.error('error with rotating orphans:')
            logger.exception(e)
            # just orphans so continue on 

    return({'successful': successful,
            'failed': failed,
            'orphans_rotated': orphan_result['orphans_rotated'],
            'orphans_failed_to_rotate': orphan_result['orphans_failed_to_rotate'],
            })


def write_status(all_result):
    """ write status to LOG_BASE_PATH/STATUS_FILENAME
    """
    status_file = open("%s/%s" % (settings.LOG_BASE_PATH,
                                  settings.STATUS_FILENAME),'w')
    # write header
    if all_result['failed']:
        status_file.write("CRITICAL some rbd devices failed to back up|")
    elif all_result['orphans_failed_to_rotate']:
        status_file.write('WARNING all rbd devices backed up successfully but'
            'some orphans failed to rotate|')
    else:
        status_file.write('OK completed successfully|')
    # now write number counts
    for key in all_result.keys():
        status_file.write("num_%s=%s " % (key, len(all_result[key])))
    # now write details in perf data format for nagios
    if all_result['failed']:
        failed_images = ["%s/%s" % (image_hash['pool'],
            image_hash['image']) for image_hash in all_result['failed']]
        failed_images_string = " ".join(["%s=failed" % image for image in
            failed_images])
        status_file.write('%s ' % failed_images_string)
    if all_result['orphans_failed_to_rotate']:
        failed_orphans = ["%s/%s" % (orphan_hash['pool'],
            orphan_hash['orphan']) for orphan_hash in
            all_result['orphans_failed_to_rotate']]
        failed_orphans_string = " ".join(["%s=orphan_failed_to_rotate" % orphan
            for orphan in failed_orphans])
        status_file.write('%s ' % failed_orphans_string)
    status_file.close()


# if not cli then check env

# enty for the rsnap node
def ceph_rsnapshot():
    # if __name__=='__main__':
    parser = argparse.ArgumentParser(description='wrapper script to backup a ceph pool of rbd images to qcow',
                                     argument_default=argparse.SUPPRESS)
    parser.add_argument("-c", "--config", required=False,
                        help="path to alternate config file")
    parser.add_argument("--host", required=False,
                        help="ceph node to backup from")
    parser.add_argument('-p', '--pools', help='comma separated list of'
                        'ceph pools to back up (can be a single pool)',
                        required=False)
    parser.add_argument('--image_re', required=False,
                        help='RE to match images to back up')
    parser.add_argument("-v", "--verbose", action='store_true',
                        required=False, help="verbose logging output")
    parser.add_argument("--noop", action='store_true', required=False,
                        help="noop - don't make any directories or do any actions. logging only to stdout")
    parser.add_argument("--no_rotate_orphans", action='store_true', required=False,
                        help="don't rotate the orphans on the dest")
    parser.add_argument("--printsettings", action='store_true',
                        required=False, help="print out settings using and exit")
    parser.add_argument("-k", "--keepconf", action='store_true',
                        required=False, help="keep conf files after run")
    parser.add_argument("-e", "--extralongargs", required=False,
                        help="extra long args for rsync of format foo,bar for arg --foo --bar")
    # TODO add param options:
    # to show names on source only
    args = parser.parse_args()

    # if we got passed an alt config file path, use that
    if args.__contains__('config'):
        config_file = args.config
        settings.load_settings(config_file)
    else:
        settings.load_settings()

    # override global settings with cli args
    # TODO get this working this way
    # for key in args.__dict__.keys():
    #   etc
    if args.__contains__('host'):
        settings.CEPH_HOST = args.host
    if args.__contains__('pools'):
        settings.POOLS = args.pools
    if args.__contains__('verbose'):
        settings.VERBOSE = args.verbose
    if args.__contains__('noop'):
        settings.NOOP = args.noop
    if args.__contains__('keepconf'):
        settings.KEEPCONF = args.keepconf
    if args.__contains__('extralongargs'):
        settings.EXTRA_ARGS = ' '.join(
            ['--' + x for x in args.extralongargs.split(',')])
        # FIXME not working correctly
    # image_filter = args.image_filter
    if args.__contains__('image_re'):
        settings.IMAGE_RE = args.image_re
    if args.__contains__('no_rotate_orphans'):
        settings.NO_ROTATE_ORPHANS = args.no_rotate_orphans

    logger = logs.setup_logging()
    logger.info("starting ceph_rsnapshot")
    logger.debug("launched with cli args: " + " ".join(sys.argv))

    try:
        helpers.validate_settings_strings()
    except NameError as e:
        logger.error('error with settings strings: %s' % e)
        sys.exit(1)


    # print out settings using and exit
    if args.__contains__('printsettings'):
        # generate SNAP_DATE for printsettings
        if settings.USE_SNAP_STATUS_FILE:
            settings.SNAP_DATE = 'TBD from SNAP_STATUS_FILE'
        else:
            # convert snap_date (might be relative) to an absolute date
            # so that it's only computed once for this entire run
            settings.SNAP_DATE = sh.date(date=settings.SNAP_DATE).strip('\n')
        # if it's there it's true
        logger.info('settings would have been:\n')
        logger.info(json.dumps(helpers.get_current_settings(), indent=2))
        logger.info('exiting')
        sys.exit(0)

    # write lockfile
    # TODO do this per ceph host or per ceph cluster
    # http://stackoverflow.com/a/789383/5928049
    pid = str(os.getpid())
    pidfile = "/var/run/ceph_rsnapshot_cephhost_%s.pid" % settings.CEPH_HOST
    if os.path.isfile(pidfile):
        logger.error("pidfile %s already exists, exiting" % pidfile)
        sys.exit(1)
    logger.info("writing lockfile at %s" % pidfile)
    file(pidfile, 'w').write(pid)

    logger.debug('running with settings:\n')
    logger.debug(json.dumps(helpers.get_current_settings(), indent=2))

    try:
        # we've made the lockfile, so rsnap the pools
        # clear this so we know if run worked or not
        all_result={}

        # check if we have been passed SNAP_STATUS_FILE
        if settings.USE_SNAP_STATUS_FILE:
            try:
                settings.SNAP_DATE = ceph.check_snap_status_file()
                logger.info('using snap date %s' % settings.SNAP_DATE)
            except exceptions.NoSnapStatusFilesFoundError as e:
                e.log(warn=True)
                raise
            except exceptions.SnapDateNotValidDateError as e:
                e.log()
                raise
            except exceptions.SnapDateFormatMismatchError as e:
                e.log()
                raise
            except Exception as e:
                logger.exception(e)
                raise
        # convert snap_date (might be relative) to an absolute date
        # so that it's only computed once for this entire run
        # FIXME does this need snap naming format
        settings.SNAP_DATE = sh.date(date=settings.SNAP_DATE).strip('\n')

        # iterate over pools
        pools_csv = settings.POOLS
        pools_arr = pools_csv.split(',')
        for pool in pools_arr:
            logger.info('working on pool "%s"' % pool)
            if len(pool) == 0:
                logger.error('empty pool name, skipping')
                continue

            # store this pool in settings for other functions to access it
            settings.POOL = pool

            # setup directories for this pool
            dirs.setup_log_dirs_for_pool(pool)
            dirs.setup_temp_conf_dir_for_pool(pool)
            dirs.setup_backup_dirs_for_pool(pool)

            # connect to ceph node and setup qcow export path
            dirs.setup_qcow_temp_path(pool)

            try:
                # TODO pass args here instead of in settings?
                pool_result = rsnap_pool(pool)
                # now append to all_result
                for key in pool_result:
                    # they are all arrays so append
                    # but we need to make the array first if not yet there
                    if not all_result.has_key(key):
                        all_result[key]=[]
                    # now append
                    all_result[key].extend(pool_result[key])
            except NameError as e:
                # TODO get some way to still have the list of images that
                # it completed before failing
                logger.error('rsnap pool %s failed error: %s' % (pool, e))
                logger.exception(e)
            except Exception as e:
                logger.error('error with pool %s' % pool)
                logger.exception(e)
            logger.info('done with pool %s' % pool)
            if not settings.KEEPCONF:
                dirs.remove_temp_conf_dir()

        # successful, so clean out snap dir
        snap_date = ceph.get_snapdate(snap_date=settings.SNAP_DATE)
        logger.info('removing snap_status file for snap_date %s on ceph host' %
                snap_date)
        ceph.remove_snap_status_file(snap_date=snap_date)

        # write output
        successful_images = [('%s/%s' % (image['pool'], image['image'])) for
            image in all_result['successful']]
        logger.info('Successfully backed up images: ' +
            ', '.join(successful_images))
        if all_result['failed']:
            logger.error("Images failed to back up:")
            failed_images_status = [{"%s/%s" % (image_hash['pool'],
                image_hash['image']): [{check: False} for check in
                image_hash['status'] if image_hash['status'][check] is False]}
                for image_hash in all_result['failed']]
            logger.error(failed_images_status)
        if all_result['orphans_rotated']:
            logger.info("orphans rotated:")
            logger.info(all_result['orphans_rotated'])
        if all_result['orphans_failed_to_rotate']:
            logger.error("orphans failed to rotate:")
            logger.error(all_result['orphans_failed_to_rotate'])
        write_status(all_result)
        logger.info("done")
    finally:
        # done with this pool so clear the pidfile
        logger.info("removing lockfile at %s" % pidfile)
        os.unlink(pidfile)

        if settings.NOOP:
            logger.info("end of NOOP run")

        # TODO should these still sys.exit or should they let the exceptions
        # go?
        if all_result:
            if all_result['failed']:
                sys.exit(1)
            elif all_result['orphans_failed_to_rotate']:
                sys.exit(2)
            else:
                sys.exit(0)
        else:
            exit(3)

# Nothing else down here - it all goes in the finally
