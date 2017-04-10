from ceph_rsnapshot import logs

class CephRsnapshotException(Exception):
    def __init__(self, *args):
        self.args = args
        self.msg = args[0]

    def __str__(self):
        return self.msg

    def explain(self):
        return '%s: %s' % (self.__class__.__name__, self.msg)

    def log(self, warn=False, show_tb=False):
        if warn:
            logs.log.warn(self.explain(), exc_info=show_tb)
        else:
            logs.log.error(self.explain(), exc_info=show_tb)


class NoSnapFilesFoundError(CephRsnapshotException):
    """
    Raised when no snap_status files found on ceph host
    """
    def __init__(self, cephhost, status_dir, e):
        self.msg = ( "No snap_status files found on ceph host {cephhost} in dir"
            " {status_dir}".format(cephhost=cephhost, status_dir=status_dir))


class SnapDateNotValidDateError(CephRsnapshotException):
    """
    Raised when a snap_date is not a valid date
    """
    def __init__(self, snap_date, date_format, e):
        self.msg = ( "Snap date: {snap_date} is not a valid date,"
                " ran: {full_cmd}, error: {stderr}".format(
                    snap_date=snap_date, full_cmd=e.full_cmd, stderr=e.stderr))


class SnapDateFormatMismatchError(CephRsnapshotException):
    """
    Raised when a snap_date does not match format
    """
    def __init__(self, snap_date, date_format, e):
        self.msg = ( "Snap date: {snap_date} does not match format:"
                " {date_format}".format(
                    snap_date=snap_date, date_format=date_format))

