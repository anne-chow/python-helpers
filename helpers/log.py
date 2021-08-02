import logging
import logging.handlers
import os
import sys

# Common Logging Utility


# level = CRITICAL/FATAL, ERROR, WARNING, INFO, DEBUG, NOTSET
def get_logger(name=None, path=None, level=None):
    if name is None:
        logger_name = get_log_file_name()
    else:
        logger_name = name

    # logger = logging.getLogger(logger_name)
    logger = logging.getLogger(logger_name)

    if not logger.hasHandlers():
        # log to stdout
        ch = logging.StreamHandler()
        ch.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))
        logger.addHandler(ch)

        # log to rotating file, max size is 10MB per file
        log_file_path = _build_log_file_path(path)
        if log_file_path is not None:
            log_file_name = os.path.join(log_file_path, logger_name)
            fh = logging.handlers.RotatingFileHandler(log_file_name,
                                                      maxBytes=1024*1024*10,
                                                      backupCount=5,
                                                      encoding="utf8")
            fh.setFormatter(logging.Formatter('%(asctime)s %(process)d [%(levelname)s] %(message)s'))
            logger.addHandler(fh)

    logger.setLevel(level.upper() if level is not None else logging.INFO)

    return logger


def set_level(logger, level):
    logger.setLevel(level)


# Derive log file name in order of 1) main script file name, 2) main module name or 3) current module name
def get_log_file_name():
    main_module = sys.modules['__main__']

    if hasattr(main_module, '__file__'):
        program_name = main_module.__file__
    elif hasattr(main_module, '__name__'):
        program_name = main_module.__name__
    else:
        program_name = None

    # use this module's name
    if program_name is None:
        program_name = __name__

    base_name = os.path.basename(program_name)

    return f"{os.path.splitext(base_name)[0]}.log"


def _build_log_file_path(path=None):
    if path:
        log_path = path
    else:
        log_path = f"{os.getenv('HOME', '.')}/logs"

    if not os.path.exists(log_path):
        try:
            os.makedirs(log_path)
        except FileExistsError:
            pass
        except OSError:
            return None

    return log_path
