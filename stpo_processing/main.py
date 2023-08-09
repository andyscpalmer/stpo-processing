import logging
from threading import Thread

from atproto.exceptions import AtProtocolError
from psycopg2 import Error as PGError

from src.constants import DEBUG, RAW_POSTS_TABLE_MODEL, STPO_MAP_MODEL, LOGGING_MODEL
from src.database import get_connection_and_cursor
# from src.firehose import AtProtocolError
from src.logging import LogDBHandler, set_local_logger
from src.process_loops import count_posts, package_message_handler, process_posts

logger = set_local_logger(__name__)


def main():
    logger.info("Starting up.")

    try:
        logger.debug("Initializing database logger.")
        db_log = LogDBHandler(LOGGING_MODEL["name"])
        db_log.setLevel(20)
        logging.getLogger("").addHandler(db_log)

        logger.debug(
            "Connecting to database to create raw and stpo_map tables (if exists)."
        )
        con, cur = get_connection_and_cursor()
        cur.create_table(cur, RAW_POSTS_TABLE_MODEL)
        cur.create_table(cur, STPO_MAP_MODEL)
        con.close()

        logger.debug("Defining task threads.")
        task1 = Thread(target=package_message_handler)
        task2 = Thread(target=process_posts)
        if DEBUG:
            task3 = Thread(target=count_posts)

        # Start threads
        logger.debug("Starting task threads.")
        task1.start()
        task2.start()
        if DEBUG:
            task3.start()

        # end all tasks
        logger.debug("Starting .join() for all tasks.")
        task1.join()
        task2.join()
        if DEBUG:
            task3.join()

        logger.info("Tasks ended. Attempting to close gracefully.")

    except RuntimeError as e:
        logger.critical("RUNTIME ERROR:", e)
        raise
    except AtProtocolError as e:
        logger.critical("ATPROTOCOL ERROR:", e)
        raise
    except PGError as e:
        logger.critical("POSTGRES ERROR:", e)
        raise
    except KeyboardInterrupt:
        # end all tasks
        logger.info("Keyboard Exit")
        logger.debug("cum")
        raise
    except Exception as e:
        logger.critical("UNKNOWN ERROR:", e)
        raise

    finally:
        db_log.close()
        logger.debug("ass")


if __name__ == "__main__":
    main()
