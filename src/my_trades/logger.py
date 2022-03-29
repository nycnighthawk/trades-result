#!/bin/python
import sys
import logging

log_formatter = logging.Formatter("%(levelname)s | %(message)s")
log_handler = logging.StreamHandler(sys.stdout)

log_handler.setLevel(logging.INFO)
log_handler.setFormatter(log_formatter)

logger = logging.getLogger(__name__)

logger.addHandler(log_handler)
logger.setLevel(logging.INFO)
