# Ceph-rsnapshot

Scripts to backup ceph rbd images to qcow via rsnapshot.

## Usage

Set the Source scripts on the ceph node, and set the Dest scripts on the backup node that will run rsnapshot. Run deps.sh and then puppet apply venv.pp on both nodes, and then pip install -r requirements.txt. Also puppet apply directories.pp on the backup node.

Run wrapper.py from on the backup node ("dest") to backup all ceph rbd images that match image_re and have snapshots from today to qcow on the backup node.

## Requirements

This script does not generate the ceph snapshots, those need to be generated externally.

## Configuration

- image_re - optional filter to select which images to back up
- min_freespace -  minimum size to leave when exporting qcow
- temp_path - path to use to export qcows temporarily
- vm backup base path - directory on rsnap node to backup vms into
- rsnap conf base path - directory for temporary rsnapshot conf files
- retain interval
- ceph source node hostname
- ceph pool
- ceph user
- ceph cluster

## Scripts

### source: gathernames

Generates a list of ceph images that have snapshots dated from today.

### source: export_qcow

Checks free space and then exports a given ceph rbd image to qcow in a temp directory.

### source: remove_qcow

Removes a qcow from temp directory.

### dest: wrapper

This will ssh to the ceph node ("source") and gather a list of vm images to backup (runs gathernames).  Then it will iterate over that list, connecting to the ceph node to export each one in turn to qcow in a temp directory (runs export qcow), and then running rsnapshot to backup that one qcow, then connecting again to the ceph node to remove the temp qcow (runs remove qcow).

VM backup images go into /<vm backup base path>/<pool>/<image name>/<daily.NN>/<image-name>.qcow2

This will also rotate orphaned images, so they will roll off after retain_interval.

Errors log to /var/log/ceph-rsnapshot and print to stdout.

Parameters:

    --host HOST           ceph node to backup from
    -p POOL, --pool POOL  ceph pool to back up
    -v, --verbose         verbose logging output
    -k, --keepconf        keep conf files after run
    -e EXTRALONGARGS, --extralongargs EXTRALONGARGS
                        extra long args for rsync of format foo,bar for arg
                        --foo --bar

## Platform

Tested on CentOS 7
