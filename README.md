# Ceph-rsnapshot

Scripts to backup ceph rbd images to qcow via rsnapshot.

## Usage

Currently this script needs to be placed on both the source (ceph node) and the dest/backup node that will run rsnapshot.

On both nodes:

    - create venv in /home/ceph_rsnapshot/venv with python 2.6 or 2.7 and source it
    - clone this repo into /home/ceph_rsnapshot/repo and pip install -e .

    - Set config file (if overriding anything) in /home/ceph_rsnapshot/config/ceph_rsnapshot.yaml

On the backup (dest) node:

    - Run /home/ceph_rsnapshot/venv/bin/ceph_rsnapshot to backup all ceph rbd images that match image_re and have snapshots from today to qcow on the backup node.

## Requirements

This script does not generate the ceph snapshots, those need to be generated externally.

The script requires rsnapshot be installed on the system already (via system packages).

## Configuration

    CEPH_HOST
    CEPH_USER
    CEPH_CLUSTER
    POOL
    QCOW_TEMP_PATH           # path for the temporary export of qcows
    EXTRA_ARGS               # extra args to pass to rsnapshot
    TEMP_CONF_DIR_PREFIX     # prefix for temp dir to store temporary rsnapshot conf files
    TEMP_CONF_DIR            # ...or can override and set whole dir
    BACKUP_BASE_PATH         # base path on the backup host to backup into
    KEEPCONF                 # keep config files after run finishes
    LOG_BASE_PATH
    LOG_FILENAME
    VERBOSE
    IMAGE_RE                 # Regex to filter ceph rbd images to back up
    RETAIN_INTERVAL
    RETAIN_NUMBER
    SNAP_NAMING_DATE_FORMAT  # date format string to pass to `date` to get snap naming; iso format %Y-%m-%d would yield names like imagename@2016-10-04
    MIN_FREESPACE            # min freespace to leave on ceph node for exporting qcow temporarily
    SH_LOGGING               # verbose log for sh module

## Entry points

### source: gathernames

Generates a list of ceph images that have snapshots dated from today.

### source: export_qcow

Checks free space and then exports a given ceph rbd image to qcow in a temp directory.

### source: remove_qcow

Removes a qcow from temp directory.

### dest: ceph_rsnapshot

This will ssh to the ceph node ("source") and gather a list of vm images to backup (runs gathernames).  Then it will iterate over that list, connecting to the ceph node to export each one in turn to qcow in a temp directory (runs export qcow), and then running rsnapshot to backup that one qcow, then connecting again to the ceph node to remove the temp qcow (runs remove qcow).

VM backup images go into /<vm backup base path>/<pool>/<image name>/<daily.NN>/<image-name>.qcow2

This will also rotate orphaned images (by running rsnap with an empty source), so they will roll off after retain_interval.

Errors log to /var/log/ceph-rsnapshot and print to stdout.

Parameters:

    optional arguments:
      -h, --help            show this help message and exit
      -c CONFIG, --config CONFIG
                            path to alternate config file
      --host HOST           ceph node to backup from
      -p POOL, --pool POOL  ceph pool to back up
      -v, --verbose         verbose logging output
      -k, --keepconf        keep conf files after run
      -e EXTRALONGARGS, --extralongargs EXTRALONGARGS
                            extra long args for rsync of format foo,bar for arg
                            --foo --bar

## Platform

Tested on CentOS 7
