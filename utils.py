import logging
import os
import sys
import inspect
import requests

_session = None

# Avoided using the build-in "basicConfig" from logger library, because it affects botocore, and everything else
LOGLEVELS = ["DEBUG", "INFO", "WARN", "WARNING", "ERROR", "CRITICAL"]
_LOGLEVEL = ["WARNING", "ERROR", "CRITICAL"]

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def _create_session():
    """
    Creates a requests session object
    :return: Requests session object
    :rtype: object
    """
    global _session
    _session = requests.Session()
    return _session


def fetch_session():
    """
    Verifies a requests session object, creates it if necessary
    :return: Requests session object
    :rtype: object
    """
    global _session
    if _session is None or not isinstance(_session, requests.Session):
        _session = _create_session()
    return _session


def set_log_level(loglevel):
    global _LOGLEVEL
    x = LOGLEVELS

    if loglevel.upper() in LOGLEVELS:
        print(f'LOG LEVEL: {loglevel}')
        _LOGLEVEL = x[x.index(loglevel.upper()):]
    else:
        print(f'Invalid Log Level, Please use :{LOGLEVELS.keys()}, defaulting to WARN')
        logging.basicConfig(level=logging.WARN)

def _channel_creator_logger(f=None, logging_level=logging.INFO, environment="dev"):
    """
    A generic logging handler for building out a logging facility.
    :param name: The name of of the application running
    :type name: str
    :param f: The file handler
    :type f: file
    :param logging_level: The standard POSIX logging levels
    :type logging_level: int
    :return: A logging facility
    :rtype: cls
    """
    module = inspect.stack()[2]
    mod_name, func_name = os.path.basename(module.filename), module.function.title()
    logger_name = f"{mod_name[:-3]}:{func_name}".lower()
    
    logger = logging.getLogger(logger_name)
    logger.propagate = False
    logger_handler = logging.StreamHandler(f)

    log_format = '%(levelname)s %(name)s: %(message)s'
    formatter = logging.Formatter(log_format)
    logger_handler.setFormatter(formatter)
    if len(logger.handlers) == 0:
        logger.addHandler(logger_handler)
    logger.setLevel(logging_level)
    return logger


def log_debug(msg):
    """
    log debug level messages.
    :param msg: The message to include in the log
    :type msg: str
    :return: None
    """
    if "DEBUG" in _LOGLEVEL:
        logger = _channel_creator_logger(f=sys.stdout, logging_level=logging.DEBUG)
        logger.debug(msg)


def log_info(msg):
    """
    log info level message.
    :param msg: The message to include in the log
    :type msg: str
    :return: None
    """
    if "INFO" in _LOGLEVEL:
        logger = _channel_creator_logger(f=sys.stdout, logging_level=logging.INFO)
        logger.info(msg)


def log_exception(msg):
    """
    log exception. (ERROR)
    :param msg: The message to include in the log
    :type msg: str
    :return: None
    """
    if "ERROR" in _LOGLEVEL:
        logger = _channel_creator_logger(f=sys.stderr, logging_level=logging.ERROR)
        logger.exception(msg)


def log_error(msg):
    """
    log red error message. 
    :param name: The name of the log
    :type msg: str
    :return: None
    """
    if "ERROR" in _LOGLEVEL:
        logger = _channel_creator_logger(f=sys.stderr, logging_level=logging.ERROR)
        logger.error(f"{bcolors.FAIL + str(msg) + bcolors.ENDC}")


def log_warn(msg):
    """
    log a yellow warning.
    :param msg: The message to include in the log
    :type msg: str
    :return: None
    """
    if "WARN" in _LOGLEVEL:
        logger = _channel_creator_logger(f=sys.stderr, logging_level=logging.WARN)
        logger.warn(f"{bcolors.WARNING + str(msg) + bcolors.ENDC}")


def log_success(msg):
    """
    log a green success message. (INFO level)
    :param msg: The message to include in the log
    :type msg: str
    :return: None
    """
    if "INFO" in _LOGLEVEL:
        logger = _channel_creator_logger(f=sys.stderr, logging_level=logging.INFO)
        logger.info(f"{bcolors.OKGREEN + str(msg) + bcolors.ENDC}")


def log_blue(msg):
    """
    log a blue info message.
    :param msg: The message to include in the log
    :type msg: str
    :return: None
    """
    if "INFO" in _LOGLEVEL:
        logger = _channel_creator_logger(f=sys.stderr, logging_level=logging.INFO)
        logger.info(f"{bcolors.OKCYAN + str(msg) + bcolors.ENDC}")
        