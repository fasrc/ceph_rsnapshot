#!/usr/bin/env python
# deletes a temp qcow

import os, sys, socket, logging
import sh
import argparse


temp_path = '/tmp/qcows'

sh_logging = False


def setup_logging(log_location='/var/log/ceph-rsnapshot/', log_filename='remove_qcow', verbose=False):
  logger = logging.getLogger(__name__)
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
  consoleHandler = logging.StreamHandler(stream=sys.stdout)
  consoleHandler.setFormatter(logFormatter)
  logger.addHandler(consoleHandler)
  if sh_logging:
    sh_logger.addHandler(consoleHandler)
  return(logger)




if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='deletes a temporary qcow')
  parser.add_argument('image')
  # parser.add_argument('--sum', dest='accumulate', action='store_const',
  #                     const=sum, default=max,
  #                     help='sum the integers (default: find the max)')
  args = parser.parse_args()
  image = args.image

  logger = setup_logging()

  logger.info("deleting temp qcow for %s" % image)

  try:
    os.remove("%s/%s.qcow2" % (temp_path, image))
  except Exception as e:
    logger.error(e.stdout)
    logger.error(e.stderr)
    raise NameError, e
  logger.info("successfully removed qcow for %s" % image)
