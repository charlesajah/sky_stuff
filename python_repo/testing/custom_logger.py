# custom_logger.py
import logging
import traceback
import sys

class CustomLogger:
    def __init__(self, log_file='custom_logger.log', log_level=logging.DEBUG, console_level=logging.INFO):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        # Create file handler which logs debug messages
        fh = logging.FileHandler(log_file)
        fh.setLevel(log_level)

        # Create console handler with a higher log level
        ch = logging.StreamHandler()
        ch.setLevel(console_level)

        # Create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

        # Set up global exception handler
        sys.excepthook = self.handle_exception

        # Disable propagation if not needed
        self.logger.propagate = False

    def handle_exception(self, exc_type, exc_value, exc_traceback):
        exc_info = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        self.logger.error("Uncaught Exception: %s", exc_info)

    def get_logger(self):
        return self.logger
