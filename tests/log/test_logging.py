import logging
from io import StringIO
from unittest.mock import patch

from faststream.log.logging import set_logger_fmt


def test_duplicates_set_formatter():
    logger = logging.getLogger(__file__)
    logger.setLevel(logging.INFO)
    log_output = StringIO()
    with patch("sys.stdout", log_output):
        set_logger_fmt(logger, fmt="%(message)s with format1")
        set_logger_fmt(logger, fmt="%(message)s with format2")

        logger.info("msg")
        assert log_output.getvalue().strip() == "msg with format1"
