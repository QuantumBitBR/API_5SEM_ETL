import logging
import os
from datetime import datetime


class Logger:
    @staticmethod
    def initialize(name):
        log_dir = os.path.join(os.path.dirname(__file__), '../../logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_filename = datetime.now().strftime(f"{name}_%Y%m%d_%H%M%S.log")
        log_filepath = os.path.join(log_dir, log_filename)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(pathname)s - %(lineno)d - %(message)s',
            handlers=[
                logging.FileHandler(log_filepath),
                logging.StreamHandler()
            ]
        )
        
        Logger.logger = logging.getLogger(name)
    
    @staticmethod
    def info(message):
        Logger.logger.info(message, stacklevel=2)
    
    @staticmethod
    def warning(message):
        Logger.logger.warning(message, stacklevel=2)
    
    @staticmethod
    def error(message):
        Logger.logger.error(message, stacklevel=2)
    
    @staticmethod
    def debug(message):
        Logger.logger.debug(message, stacklevel=2)