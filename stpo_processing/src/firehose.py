
from atproto import CAR, models
from atproto.firehose import FirehoseSubscribeReposClient, parse_subscribe_repos_message

from src.constants import RAW_POSTS_TABLE_MODEL
from src.database import get_connection_and_cursor
from src.logging import set_local_logger

logger = set_local_logger(__name__)


class FirehoseClient(FirehoseSubscribeReposClient):
    def __init__(self):
        try:
            FirehoseSubscribeReposClient.__init__(self)
            self.con, self.cur = get_connection_and_cursor()
        except Exception as e:
            logger.warning("Exception in client init:", e)
            raise

    def on_message_handler(self, message):
        try:
            commit = parse_subscribe_repos_message(message)
            # Make sure that it's commit message with .blocks inside
            if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
                return

            car = CAR.from_bytes(commit.blocks)

            for block in car.blocks.values():
                if "$type" in block.keys():
                    if block["$type"] == "app.bsky.feed.post":
                        text = block["text"]
                        if len(text) > 2:
                            table_row = {
                                "table_name": RAW_POSTS_TABLE_MODEL["name"],
                                "column_data": [
                                    {"name": "raw_post_text", "value": text}
                                ],
                            }
                            self.cur.insert_into_table(self.cur, table_row)
        except Exception as e:
            logger.warning("Exception in message handler:", e)

    def drink_from_firehose(self):
        try:
            self.start(self.on_message_handler)
        except Exception as e:
            logger.warning("Exception drinking from firehose:", e)
            raise

    def close_db_connection(self):
        try:
            self.con.close()
        except Exception as e:
            logger.warning("Exception closing db connection:", e)
            raise
