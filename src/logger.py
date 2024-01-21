import logging


def SetUpLogger():
    logging.root.setLevel(logging.INFO)
    logging.basicConfig(level=logging.INFO)
