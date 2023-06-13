""" Script that contains all frequently used utility functions """
import logging


from logging.handlers import RotatingFileHandler

from app import config


def setup_logging(logger_name, logging_params):
    """Setup logging"""
    log_file_name = logging_params["log_file_name"]
    # file_mode = logging_params["file_mode"]
    max_bytes = logging_params["max_bytes"]
    backup_count = logging_params["backup_count"]
    # log_format = "%(asctime)s | %(levelname)s | %(message)s"

    logging.basicConfig(
        handlers=[
            RotatingFileHandler(
                log_file_name, maxBytes=max_bytes, backupCount=backup_count
            )
        ],
        level=logging.INFO,
        format="[%(asctime)s] | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d:%H:%M:%S",
    )

    logger = logging.getLogger()
    return logger


# UTILS_LOGGER = setup_logging("utils", config.SHARED_LOGGING_PARAMETERS)
