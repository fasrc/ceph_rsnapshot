# Ceph-rsnapshot

Scripts to backup ceph rbd images to qcow via rsnapshot. Tunable image selection with a regex (current default setup for opennebula vm root disks and base images).

## Usage

This script is to be installed on the backup node, and it will connect via ssh
to the ceph node.

Setup on the backup node:

    - create venv in /home/ceph_rsnapshot/venv with python 2.6 or 2.7 and source it
    - clone this repo into /home/ceph_rsnapshot/repo and pip install -e .

    - Set config file (if overriding anything) in /home/ceph_rsnapshot/config/ceph_rsnapshot.yaml

    - Run /home/ceph_rsnapshot/venv/bin/ceph_rsnapshot to backup all ceph rbd images that match image_re and have snapshots from today's date to qcow on the backup node.

## Requirements

- This script does not generate the ceph snapshots, those need to be generated externally.

- The script requires rsnapshot be installed on the backup node already (via system packages).

- This also requires qemu-img to be installed on the ceph node.

- requires passwordless ssh from the backup node to the ceph node

## Configuration

    CEPH_HOST
    CEPH_USER
    CEPH_CLUSTER
    POOLS                    # comma separated list of pools to backup; can be a single pool
    QCOW_TEMP_PATH           # path for the temporary export of qcows
    EXTRA_ARGS               # extra args to pass to rsnapshot
    TEMP_CONF_DIR_PREFIX     # prefix for temp dir to store temporary rsnapshot conf files
    TEMP_CONF_DIR            # ...or can override and set whole dir
    BACKUP_BASE_PATH         # base path on the backup host to backup into
    KEEPCONF                 # keep config files after run finishes
    LOG_BASE_PATH
    LOG_FILENAME
    VERBOSE
    NOOP
    NO_ROTATE_ORPHANS
    IMAGE_RE                 # Regex to filter ceph rbd images to back up
    RETAIN_INTERVAL
    RETAIN_NUMBER
    SNAP_NAMING_DATE_FORMAT  # date format string to pass to `date` to get snap naming; iso format %Y-%m-%d would yield names like imagename@2016-10-04
    MIN_FREESPACE            # min freespace to leave on ceph node for exporting qcow temporarily
    SH_LOGGING               # verbose log for sh module


## Entry points

### ceph_rsnapshot

This will ssh to the ceph node ("source") and gather a list of rbd devices to back up.  Then it will iterate over that list, connecting to the ceph node to export each one in turn to qcow in a temp directory, and then running rsnapshot to backup that one qcow, then connecting again to the ceph node to remove the temp qcow.

The qcow images go into (on the backup node): <BACKUP_BASE_PATH>/<POOL>/<image-name>/<daily.NN>/<image-name>.qcow2

This script will also rotate orphaned images that no longer exist on the source (by running rsnap with an empty source), so they will roll off after retain_interval.

Log messages print to stdout and log to /home/ceph_rsnapshot/logs (or LOG_BASE_PATH).

Parameters:

    usage: ceph_rsnapshot [-h] [-c CONFIG] [--host HOST] [-p POOL]
                          [--image_re IMAGE_RE] [-v] [--noop]
                          [--no_rotate_orphans] [--printsettings] [-k]
                          [-e EXTRALONGARGS]
    
    wrapper script to backup a ceph pool of rbd images to qcow
    
    optional arguments:
      -h, --help            show this help message and exit
      -c CONFIG, --config CONFIG
                            path to alternate config file
      --host HOST           ceph node to backup from
      -p POOLS, --pools POOLS
                            comma separated list ofceph pools to back up (can be a
                            single pool)
      --image_re IMAGE_RE   RE to match images to back up
      -v, --verbose         verbose logging output
      --noop                noop - don't make any directories or do any actions.
                            logging only to stdout
      --no_rotate_orphans   don't rotate the orphans on the dest
      --printsettings       print out settings using and exit
      -k, --keepconf        keep conf files after run
      -e EXTRALONGARGS, --extralongargs EXTRALONGARGS
                            extra long args for rsync of format foo,bar for arg
                            --foo --bar

## Platform

Tested on CentOS 7 with python 2.7.5
