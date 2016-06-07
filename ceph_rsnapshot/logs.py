# logging setup

import sh, sys, os, socket, logging

sh_logging = False


def setup_logging(log_filename, log_location='/var/log/ceph-rsnapshot/', verbose=False):
  logger = logging.getLogger('ceph_rsnapshot')
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
  consoleHandler = logging.StreamHandler(stream=sys.stderr)
  consoleHandler.setFormatter(logFormatter)
  logger.addHandler(consoleHandler)
  if sh_logging:
    sh_logger.addHandler(consoleHandler)
  return(logger)