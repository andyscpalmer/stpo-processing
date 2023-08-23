from datetime import datetime, timedelta, timezone

from psycopg2 import Error as PGError

from src.constants import LOGGING_MODEL, RAW_POSTS_TABLE_MODEL
from src.database import get_connection_and_cursor
from src.logging import set_local_logger

logger = set_local_logger(__name__)


def get_posts_count():
    try:
        con, cur = get_connection_and_cursor()

        select_post_num = {
            "table_name": RAW_POSTS_TABLE_MODEL["name"],
            "text": "count(*)",
        }
        results = cur.select_from_table(cur, select_post_num)
        if results:
            count = results[0][0]
        else:
            count = 0
    except PGError as e:
        logger.error("Postgres error counting posts:", e)
        raise
    except Exception as e:
        logger.critical("Unknown Error counting posts:", e)
        raise
    finally:
        con.close()

    return count

def clean_up_database() -> None:
    current_time = datetime.now(timezone.utc)

    try:
        con, cur = get_connection_and_cursor()

        # Delete posts older than two days
        posts_interval = timedelta(days=2)
        posts_delete_attrs = {
            "table_name": RAW_POSTS_TABLE_MODEL["name"],
            "where": [
                {
                    "column": "created_at",
                    "operator": "<",
                    "value": current_time - posts_interval,
                }
            ],
        }
        cur.delete_from_table(cur, posts_delete_attrs)
        logger.info(
            f"Deleted posts older than {posts_interval}."
        )

        # Delete logs older than two days
        logs_interval = timedelta(days=2)
        logs_delete_attrs = {
            "table_name": LOGGING_MODEL["name"],
            "where": [
                {
                    "column": "created_at",
                    "operator": "<",
                    "value": current_time - logs_interval,
                }
            ],
        }
        cur.delete_from_table(cur, logs_delete_attrs)
        logger.info(
            f"Deleted logs older than {logs_interval}."
        )

        #
    except PGError as e:
        logger.error("Posgres Error:", e)
    except Exception as e:
        logger.error("Unknown exception:", e)
    finally:
        con.close()