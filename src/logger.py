import logging

def SetUpLogger():
        logging.basicConfig()
        logging.root.setLevel(logging.INFO)
        logging.basicConfig(level=logging.INFO)