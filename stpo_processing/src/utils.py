from psycopg2 import Error as PGError

from src.constants import RAW_POSTS_TABLE_MODEL
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
