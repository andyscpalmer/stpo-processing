import concurrent.futures
from datetime import datetime, timedelta, timezone
import json
import time

from atproto.exceptions import AtProtocolError
from psycopg2 import Error as PGError

from src.constants import (
    RAW_POSTS_TABLE_MODEL,
    STPO_MAP_MODEL,
    RETRY_COUNT,
    RETRY_DELAY,
)
from src.database import get_connection_and_cursor
from src.firehose import FirehoseClient
from src.logging import set_local_logger
from src.raw_post_processing import orchestrate_stpo
from src.utils import get_posts_count, clean_up_database

logger = set_local_logger(__name__)


# def loop_decorator(func):
#     logger.info(f"Starting {func.__name__}.")
#     retry_count = RETRY_COUNT
#     retries = 0
#     posts_count = get_posts_count()

#     try:
#         while retries < retry_count:
#             try:
#                 new_posts_count = get_posts_count()
#                 func()
#             except AtProtocolError as e:
#                 logger.error("Message Handler error:", e)
#             except PGError as e:
#                 logger.error("Posgres Error:", e)
#             except Exception as e:
#                 logger.error("Unknown exception:", e)
#             finally:
#                 if new_posts_count > posts_count:
#                     retries = 0
#                     posts_count = new_posts_count
#                 else:
#                     retries += 1

#                 if retries < retry_count:
#                     logger.error(f"Restarting. (Retries: {retries}/{retry_count})")
#                 time.sleep(RETRY_DELAY)
#     except Exception as e:
#         logger.critical(f"{func.__name__.upper()} EXCEPTION: {e}\n\nKilling.")
#         raise


def firehose_message_handler():
    logger.info("Starting firehose message handler.")
    retry_count = RETRY_COUNT
    retries = 0
    posts_count = get_posts_count()

    try:
        while retries < retry_count:
            try:
                new_posts_count = get_posts_count()
                client = FirehoseClient()
                client.drink_from_firehose()
            except AtProtocolError as e:
                logger.error("Message Handler error:", e)
            except PGError as e:
                logger.error("Posgres Error:", e)
            except Exception as e:
                logger.error("Unknown exception:", e)
            finally:
                if new_posts_count > posts_count:
                    retries = 0
                    posts_count = new_posts_count
                else:
                    retries += 1

                if retries < retry_count:
                    logger.error(f"Restarting. (Retries: {retries}/{retry_count})")
                time.sleep(RETRY_DELAY)
    except Exception as e:
        logger.critical(f"FIREHOSE EXCEPTION: {e}\n\nKilling.")
        raise


def count_posts():
    logger.info("Starting minute by minute post counter.")
    retry_count = RETRY_COUNT
    retries = 0
    posts_count = get_posts_count()

    try:
        while retries < retry_count:
            try:
                new_posts_count = get_posts_count()

                # Content for future decorator
                interval = 0.2
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
                            last_minute_of_posts = {
                                "table_name": RAW_POSTS_TABLE_MODEL["name"],
                                "count": "raw_post_text",
                                "where": [
                                    {
                                        "column": "created_at",
                                        "operator": ">",
                                        "value": current_time - timedelta(minutes=1),
                                    }
                                ],
                            }
                            results = cur.select_from_table(cur, last_minute_of_posts)
                            if results:
                                num_posts = results[0][0]
                                logger.info(f"Posts in last minute: {num_posts}")
                                logger.debug(f"Total post count: {get_posts_count()}")
                                previous_time = current_time
                            else:
                                logger.info(f"No posts from the last minute retrieved.")
                        except PGError as e:
                            logger.error("Postgres Error:", e)
                            raise
                        except Exception as e:
                            logger.critical("Unknown Error counting posts:", e)
                            raise

                    time.sleep(interval)
                # End content for future decorator

            except AtProtocolError as e:
                logger.error("Message Handler error:", e)
            except PGError as e:
                logger.error("Posgres Error:", e)
            except Exception as e:
                logger.error("Unknown exception:", e)
            finally:
                if new_posts_count > posts_count:
                    retries = 0
                    posts_count = new_posts_count
                else:
                    retries += 1

                if retries < retry_count:
                    logger.error(f"Restarting. (Retries: {retries}/{retry_count})")
                time.sleep(RETRY_DELAY)
    except Exception as e:
        logger.critical(f"FIREHOSE EXCEPTION: {e}\n\nKilling.")
        raise


def process_posts():
    logger.info("Starting post processor.")
    retry_count = RETRY_COUNT
    retries = 0
    posts_count = get_posts_count()

    try:
        while retries < retry_count:
            try:
                new_posts_count = get_posts_count()

                # Content for future decorator
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
                                "table_name": RAW_POSTS_TABLE_MODEL["name"],
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
                            results = cur.select_from_table(cur, last_day_of_posts)
                            if results:
                                logger.debug("Building STPO map.")
                                process_start = datetime.now()
                                posts = [result[0] for result in results]
                                logger.debug(f"{len(posts)} posts retrieved")
                                if not posts:
                                    logger.warning("NO POSTS COMING THROUGH")
                                else:
                                    executor = concurrent.futures.ThreadPoolExecutor(
                                        max_workers=1
                                    )
                                    process = executor.submit(
                                        orchestrate_stpo, posts
                                    )
                                    stpo_map = process.result()

                                    process_end = datetime.now()
                                    process_interval = process_end - process_start
                                    logger.info(
                                        f"STPO map built in {process_interval.seconds} seconds"
                                    )

                                    if "post" in stpo_map.keys():
                                        logger.info(
                                            "Words following 'post':\n"
                                            f"{json.dumps(stpo_map[1]['post'], indent=2)}"
                                        )

                                    stpo_json = json.dumps(stpo_map)
                                    table_row = {
                                        "table_name": STPO_MAP_MODEL["name"],
                                        "column_data": [
                                            {
                                                "name": "stpo_snapshot",
                                                "value": stpo_json,
                                            },
                                            {
                                                "name": "snapshot_interval",
                                                "value": analysis_interval,
                                            },
                                            {
                                                "name": "created_at",
                                                "value": current_time,
                                            },
                                        ],
                                    }
                                    cur.insert_into_table(cur, table_row)
                                    logger.info("JSON successfully saved.")

                                    # Delete old posts and logs
                                    clean_up_database()
                        except PGError as e:
                            logger.error("Postgres Error:", e)
                            raise
                        except Exception as e:
                            logger.critical("Unknown Error counting posts:", e)
                            raise
                        finally:
                            con.close()

                    time.sleep(interval)
                # End content for future decorator

            except AtProtocolError as e:
                logger.error("Message Handler error:", e)
            except PGError as e:
                logger.error("Posgres Error:", e)
            except Exception as e:
                logger.error("Unknown exception:", e)
            finally:
                if new_posts_count > posts_count:
                    retries = 0
                    posts_count = new_posts_count
                else:
                    retries += 1

                if retries < retry_count:
                    logger.error(f"Restarting. (Retries: {retries}/{retry_count})")
                time.sleep(RETRY_DELAY)
    except Exception as e:
        logger.critical(f"FIREHOSE EXCEPTION: {e}\n\nKilling.")
        raise

