from ceph_rsnapshot import settings, logs, dirs
import tempfile
import sys
import os

import jinja2


def get_template():
    logger = logs.get_logger()
    env = jinja2.Environment(loader=jinja2.PackageLoader('ceph_rsnapshot'))
    template = env.get_template('rsnapshot.template')
    return template


def write_conf(image, pool='', source='', template=''):
    if not pool:
        pool = settings.POOL
    host = settings.CEPH_HOST
    # temp_path: note the . needed to set where to relative from
    temp_path = settings.QCOW_TEMP_PATH
    backup_base_path = settings.BACKUP_BASE_PATH

    # get logger we setup earlier
    logger = logs.get_logger()

    # only reopen template if we don't have it - ie, are we part of a pool run
    if not template:
        template = get_template()

    # create source path string if an override wasn't passed to us
    # set the . to get rsync to do relative from there
    if source == '':
        source = 'root@%s:%s/%s/./%s.qcow2' % (
            settings.CEPH_HOST, settings.QCOW_TEMP_PATH, pool, image)

    destination = '%s/%s/%s' % (settings.BACKUP_BASE_PATH,
                                settings.POOL, image)

    logger.info('writing conf for image %s to rsnap from %s to %s' %
                (image, source, destination))

    my_template = template.render(nickname=image,
                                  pool=pool,
                                  source=source,
                                  destination=destination,
                                  retain_interval=settings.RETAIN_INTERVAL,
                                  retain_number=settings.RETAIN_NUMBER,
                                  log_base_path=settings.LOG_BASE_PATH,
                                  subdir='',
                                  extra_args=settings.EXTRA_ARGS)

    if settings.NOOP:
        logger.info('NOOP: would have written conf file to %s/%s/%s.conf' %
                    (settings.TEMP_CONF_DIR, pool, image))
        logger.info('NOOP: conf file contents would have been: \n%s' %
                    my_template)
        # fake conf file name to return
        return '%s/%s/%s.conf' % (settings.TEMP_CONF_DIR, pool, image)
    else:
        # conf file of the form /tmp_conf_dir/pool/imagename.conf
        conf_file = open('%s/%s/%s.conf' %
                         (settings.TEMP_CONF_DIR, pool, image), 'w')
        # FIXME raise error if error
        conf_file.write(my_template)
        # FIXME raise error if error
    return conf_file.name

# note using mkdtemp to make a tmep dir for these so not using mkstemp
# mkstemp
# fdopen


def remove_conf(image, pool=''):
    if not pool:
        pool = settings.POOL
    # get logger we setup earlier
    logger = logs.get_logger()
    if settings.NOOP:
        logger.info('NOOP: would have removed conf file %s/%s/%s.conf' %
                    (settings.TEMP_CONF_DIR, pool, image))
    else:
        os.remove('%s/%s/%s.conf' % (settings.TEMP_CONF_DIR, pool, image))
        # FIXME raise error if error


def test_template():
    settings.load_settings()
    settings.LOG_BASE_PATH = '/tmp/ceph_rsnapshot_logs'
    logger = logs.setup_logging()
    temp_conf_dir = dirs.setup_temp_conf_dir()
    logger.info('temp conf dir is %s' % temp_conf_dir)
    print(settings.TEMP_CONF_DIR)

    get_template()
