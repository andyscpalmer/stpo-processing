import logging
import sys


from .database import get_connection_and_cursor
from .constants import DEBUG, LOGGING_MODEL


# Set stdout stream logger to logging root
# logFormatter = logging.Formatter(
#     "[%(asctime)s] [%(threadName)-15.15s] [%(levelname)-5.5s]  %(message)s"
# )
logFormatter = logging.Formatter(
    "[%(threadName)-10.10s][%(levelname)-5.5s][%(module)-8.8s]  %(message)s"
)
rootLogger = logging.getLogger()

consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)


def set_local_logger(name: str):
    logger = logging.getLogger(name)
    if DEBUG:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    return logger


logger = set_local_logger(__name__)

logger.debug("Logger spot check")


class LogDBHandler(logging.Handler):
    """
    log_db_model = {
        "name": "operation_logs",
        "temp": False,
        "is_if_not_exists": True,
        "columns": [
            {
                "name": "id",
                "data_type": "serial",
                "is_null": False,
                "constraint": "primary_key"
            },
            {

            }
        ]
    }
    """

    def __init__(self, table_name):
        try:
            logging.Handler.__init__(self)
            con, cur = get_connection_and_cursor()
            self.cur = cur
            self.con = con
            self.table_name = table_name
            cur.create_table(cur, LOGGING_MODEL)
        except Exception as e:
            logger.critical("ERROR CREATING HANDLER:", e)
            raise

    def emit(self, record):
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace("'", "''")

        table_rows = {
            "table_name": LOGGING_MODEL["name"],
            "column_data": [
                {"name": "log_level", "value": int(record.levelno)},
                {"name": "log_levelname", "value": str(record.levelname)},
                {"name": "log", "value": str(self.log_msg)},
                {"name": "created_by", "value": str(record.name)},
            ],
        }

        try:
            self.cur.insert_into_table(self.cur, table_rows)
        except Exception as e:
            # logger.critical("DB ERROR! Logging to database not possible!")
            exit()
            logger.critical("ERROR: ", e)
            raise

    def close(self):
        try:
            self.con.close()
        except Exception as e:
            logger.critical(e)
            raise
