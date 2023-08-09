import concurrent.futures
from datetime import datetime, timedelta, timezone
import json
import time

from atproto.exceptions import AtProtocolError
from psycopg2 import Error as PGError

from src.constants import DEBUG, RAW_POSTS_TABLE_MODEL, STPO_MAP_MODEL
from src.database import get_connection_and_cursor
from src.firehose import FirehoseClient
from src.logging import set_local_logger
from src.raw_post_processing import orchestrate_stpo

logger = set_local_logger(__name__)

# TODO: Input an error counter over time to only quit if frequency indicates
# a systemic issue and/or a runaway loop


def package_message_handler():
    logger.info("Starting message handler")
    try:
        while True:
            try:
                client = FirehoseClient()
                client.drink_from_firehose()
            except AtProtocolError as e:
                logger.error("Message Handler error:", e)
                logger.error("Restarting.")
            except PGError as e:
                logger.error("Posgres Error:", e)
                logger.error("Restarting.")
            finally:
                client.close_db_connection()

    except Exception as e:
        logger.critical("MESSAGE HANDLER EXCEPTION:", e)
        raise


def count_posts():
    logger.info("Starting post counter")
    interval = 0.2
    previous_post_count = 0
    previous_time = datetime.now(timezone.utc)
    two_seconds = timedelta(seconds=2)
    while True:
        current_time = datetime.now(timezone.utc)
        loop_interval = current_time - previous_time
        is_over_two_sec = loop_interval > two_seconds
        is_new_minute = current_time.second == 0

        if is_over_two_sec and is_new_minute:
            try:
                con, cur = get_connection_and_cursor()
                previous_time = datetime.now(timezone.utc)

                select_post_num = {
                    "table_name": RAW_POSTS_TABLE_MODEL["name"],
                    "text": "count(*)",
                }
                results = cur.select_from_table(cur, select_post_num)
                if results:
                    count = results[0][0]
                    intermediate_posts = count - previous_post_count
                    previous_post_count = count
                    logger.info(f"Posts in last minute: {intermediate_posts}")
                    logger.debug(f"Total post count: {count}")
            except PGError as e:
                logger.error("Postgres Error. Likely non-critical:", e)
                logger.error("Restarting.")
            except Exception as e:
                logger.critical("Unknown Error counting posts:", e)
                raise
            finally:
                con.close()

        time.sleep(interval)


def process_posts():
    logger.info("Starting post processor")
    interval = 1
    previous_time = datetime.now(timezone.utc)
    two_minutes = timedelta(seconds=120)

    while True:
        # Check if it's a ten and if it's greater than two mins
        current_time = datetime.now(timezone.utc)
        loop_interval = current_time - previous_time
        is_over_two_min = loop_interval > two_minutes
        is_ten = current_time.minute % 10 == 0

        if is_ten and is_over_two_min:
            try:
                logger.info("Begin STPO processing")
                con, cur = get_connection_and_cursor()
                previous_time = current_time
                analysis_interval = timedelta(days=1)

                last_day_of_posts = {
                    "table_name": "raw_post_data",
                    "columns": ["raw_post_text"],
                    "where": [
                        {
                            "column": "created_at",
                            "operator": ">",
                            "value": current_time - analysis_interval,
                        }
                    ],
                }
                logger.debug("Getting posts.")
                results = cur.select_from_table(cur, last_day_of_posts, verbose=DEBUG)
                if results:
                    logger.debug("Building STPO map.")
                    process_start = datetime.now()
                    posts = [result[0] for result in results]
                    logger.debug(f"{len(posts)} posts retrieved")
                    if not posts:
                        logger.warning("NO POSTS COMING THROUGH")
                    else:
                        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                        process = executor.submit(orchestrate_stpo, posts, True)
                        stpo_map = process.result()

                        process_end = datetime.now()
                        process_interval = process_end - process_start
                        logger.info(
                            f"STPO map built in {process_interval.seconds} seconds"
                        )

                        if "post" in stpo_map.keys():
                            logger.info(f"Words following 'post':\n{json.dumps(stpo_map[1]['post'], indent=2)}")

                        stpo_json = json.dumps(stpo_map)
                        table_row = {
                            "table_name": STPO_MAP_MODEL["name"],
                            "column_data": [
                                {"name": "stpo_snapshot", "value": stpo_json},
                                {
                                    "name": "snapshot_interval",
                                    "value": analysis_interval,
                                },
                                {"name": "created_at", "value": current_time},
                            ],
                        }
                        cur.insert_into_table(cur, table_row)
                        logger.info("JSON successfully saved.")
            except PGError as e:
                logger.error("Postgres Error:", e)
                logger.info("Restarting.")
            finally:
                con.close()

        time.sleep(interval)
