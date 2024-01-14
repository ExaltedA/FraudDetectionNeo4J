from database import Database
from generator import generate_all_datasets, dir_error_handler
from config import Config

from logger import SetUpLogger
import logging


# GLOBAL CONSTANTS
DIR_DATA = "./data"
DIR_OUTPUT = "./output"
START_DATE = "2022-01-01"
RADIUS = 5

if __name__ == "__main__":
    # Set up logger settings
    SetUpLogger()
    logger = logging.getLogger("generator")

    # Check if file does not exists
    dir_error_handler(DIR_DATA)

    open(f"{DIR_DATA}/generator_log.txt", "w")

    logger.addHandler(logging.FileHandler(f"{DIR_DATA}/generator_log.txt"))
    
    dataset_template = {100: (2500,  5000, 365),
                        200: (5000, 10000, 365),
                        300: (7500, 15000, 365)}
    # Generate all datasets
    # generate_all_datasets(dataset_template, DIR_DATA, START_DATE, RADIUS)

    # setup connection, load and query the database
    for size in [100, 200, 300]:
        config = Config(size)
        db = Database(config.Url, config.User,
                      config.Password, f"{DIR_OUTPUT}/{size}")
        
        try:
            db.load_customer(f"file:///{size}/customer.csv")
            db.index_customer()

            db.load_terminal(f"file:///{size}/terminal.csv")
            db.index_terminal()

            db.load_transaction(f"data/{size}/transaction.csv")
            db.index_transaction()

            db.query_1()
            db.query_2()
            db.query_3()
            db.query_4_1()
            db.query_4_2()
            db.query_4_3()
            db.query_5()

        finally:
            db.close()
