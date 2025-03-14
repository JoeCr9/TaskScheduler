import logging
import os

def setup_logging():
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    info_handler = logging.FileHandler('logs/scheduler.log')
    info_handler.setFormatter(formatter)

    error_handler = logging.FileHandler('logs/scheduler_errors.log')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not logger.hasHandlers():
        logger.addHandler(info_handler)
        logger.addHandler(error_handler)
    