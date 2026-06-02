import os
import logging
from logging.handlers import TimedRotatingFileHandler

# Create logs directory if not exists
#LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
LOG_DIR = "/content/drive/MyDrive/Colab Work/IIT MADRAS/TERM 2/AI_Lab/logs"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "project.log")

def get_logger(name: str):
    """
    Returns a configured logger with file and console handlers.
    """

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Prevent duplication of handlers
    if logger.hasHandlers():
          for h in logger.handlers:
            logger.removeHandler(h)

    # ---- Console Handler ----
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    console_handler.setFormatter(console_format)

    # File Handler using FileHandler instead of RotatingFileHandler
    #file_handler = logging.FileHandler(LOG_FILE, mode="a")
    file_handler = TimedRotatingFileHandler(LOG_FILE, when='midnight', backupCount=5)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(message)s"
    ))

    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
