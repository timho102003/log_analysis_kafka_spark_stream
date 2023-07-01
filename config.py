import logging
from pathlib import Path


host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
ts_pattern = r'\[(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\]'
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
status_pattern = r'\s(\d{3})\s'
content_size_pattern = r'\s(\d+)$'

day_of_week = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']

# Terminal Color:
class bcolors:
    HEADER = "\033[95m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"

class myLogger:
    def __init__(
        self,
        logfmt="%(asctime)s: %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
        datefmt="%Y-%m-%d:%H:%M:%S",
        saveto=None
    ):
        '''
        Initialize the custom logger.

        Arguments:
        - logfmt: The format of the log messages. Default is "%(asctime)s: %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s".
        - datefmt: The format of the timestamp in the log messages. Default is "%Y-%m-%d:%H:%M:%S".
        - saveto: The file path to save the log messages. Default is None.
        '''
        # create logger
        loggerName = Path(__file__).stem
        # create logging formatter
        logFormatter = logging.Formatter(
            fmt=logfmt,
            datefmt=datefmt,
        )
        loglvl = getattr(logging, "INFO")
        self.logger = logging.getLogger(loggerName)
        self.logger.setLevel(loglvl)
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(loglvl)
        consoleHandler.setFormatter(logFormatter)
        self.logger.addHandler(consoleHandler)
        # Create a FileHandler and set its level
        if saveto:
            fileHandler = logging.FileHandler(saveto)
            fileHandler.setLevel("DEBUG")
            fileHandler.setFormatter(logFormatter)
            self.logger.addHandler(fileHandler)

    def info(self, msg, bold=False):
        '''
        Log an info level message.

        Arguments:
        - msg: The message to be logged.
        - bold: Whether to display the message in bold. Default is False.
        '''
        self.logger.info(
            f"{bcolors.BOLD if bold else ''}{msg}{bcolors.ENDC if bold else ''}"
        )

    def debug(self, msg, bold=False):
        '''
        Log a debug level message.

        Arguments:
        - msg: The message to be logged.
        - bold: Whether to display the message in bold. Default is False.
        '''
        self.logger.debug(
            f"{bcolors.BOLD if bold else ''}{msg}{bcolors.ENDC if bold else ''}"
        )

    def header(self, msg, bold=True):
        '''
        Log a header message.

        Arguments:
        - msg: The message to be logged as a header.
        - bold: Whether to display the message in bold. Default is True.
        '''
        self.logger.info(
            f"{bcolors.BOLD if bold else ''}{bcolors.HEADER}{msg}{bcolors.ENDC if bold else ''}{bcolors.ENDC}"
        )

    def warning(self, msg, bold=True):
        '''
        Log a warning level message.

        Arguments:
        - msg: The message to be logged as a warning.
        - bold: Whether to display the message in bold. Default is True.
        '''
        self.logger.warning(
            f"{bcolors.BOLD if bold else ''}{bcolors.WARNING}{msg}{bcolors.ENDC if bold else ''}{bcolors.ENDC}",
        )

    def fail(self, msg, bold=True):
        '''
        Log a failure message.

        Arguments:
        - msg: The message to be logged as a failure.
        - bold: Whether to display the message in bold. Default is True.
        '''
        self.logger.error(
            f"{bcolors.BOLD if bold else ''}{bcolors.FAIL}{msg}{bcolors.ENDC if bold else ''}{bcolors.ENDC}",
            exc_info=True
        )

    def divider(self, num=50):
        '''
        Log a divider line.

        Arguments:
        - num: The number of characters in the divider line. Default is 50.
        '''
        self.logger.info(
            f"{bcolors.BOLD}{'=' * num}{bcolors.ENDC}",
        )

__all__ = ["myLogger", "host_pattern", "ts_pattern", "method_uri_protocol_pattern", "status_pattern", "content_size_pattern", "day_of_week"]
