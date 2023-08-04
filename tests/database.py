from datetime import datetime, timedelta
import sys

import psycopg2

sys.path.append("./stpo_processing")
from src.database import get_database_credentials, STPOCursor


def create_table(cur, table_attributes):
    cur.create_table(cur, table_attributes, verbose=True)


def insert_into_table(cur, table_rows):
    for table_row in table_rows:
        cur.insert_into_table(cur, table_row, verbose=True)


def select_from_table(cur, select_attrs, dict_output=False):
    result = cur.select_from_table(cur, select_attrs, dict_output=dict_output, verbose=True)
    return result


def main():
    db_creds = get_database_credentials()
    con = psycopg2.connect(**db_creds)
    cur = con.cursor(cursor_factory=STPOCursor)
    con.autocommit = True

    try:
        # Create table from attributes
        table_attributes = {
            "name": "test_table",
            "temp": False,
            "is_if_not_exists": True,
            "columns": [
                {
                    "name": "id",
                    "data_type": "serial",
                    "is_null": False,
                    "constraint": "primary key"
                },
                {
                    "name": "example_text",
                    "data_type": "text",
                    "is_null": False
                },
                {
                    "name": "created_at",
                    "data_type": "timestamp without time zone",
                    "default": "(now() at time zone 'utc')",
                    "is_null": False
                }
            ]
        }
        create_table(cur, table_attributes)

        # Insert values into table
        now = datetime.now()
        three_seconds = timedelta(seconds=3)
        timestamps = [now - three_seconds, now, now + three_seconds]

        table_rows = [
            {
                "table_name": "test_table",
                "column_data": [
                    {"name": "example_text", "value": "test1"},
                    {"name": "created_at", "value": timestamps[0]}
                ]
            },
            {
                "table_name": "test_table",
                "column_data": [
                    {"name": "example_text", "value": "test2"},
                    {"name": "created_at", "value": timestamps[1]}
                ]
            },
            {
                "table_name": "test_table",
                "column_data": [
                    {"name": "example_text", "value": "test3"},
                    {"name": "created_at", "value": timestamps[2]}
                ]
            }
        ]
        insert_into_table(cur, table_rows)

        # Get data from table and compare it to inputs
        select_attrs1 = {
            "table_name": "test_table",
            "columns": ["example_text", "created_at"]
        }
        results1 = select_from_table(cur, select_attrs1, dict_output=True)

        for i in range(len(results1)):
            assert results1[i]["example_text"] == table_rows[i]["column_data"][0]["value"]
            assert results1[i]["created_at"] == table_rows[i]["column_data"][1]["value"]

        select_attrs2 = {
            "table_name": "test_table",
            "columns": ["example_text"],
            "where": [{
                "column": "created_at",
                "operator": "<",
                "value": timestamps[2]
            }]
        }
        results2 = select_from_table(cur, select_attrs2, dict_output=True)

        assert results2[0]["example_text"] == "test1"
        assert results2[1]["example_text"] == "test2"

        select_attrs3 = {
            "table_name": "test_table",
            "columns": ["example_text"],
            "where": [
                {
                    "column": "created_at",
                    "operator": ">",
                    "value": timestamps[0]
                },
                {
                    "column": "created_at",
                    "operator": "<",
                    "value": timestamps[2]
                }
            ]
        }
        results3 = select_from_table(cur, select_attrs3, dict_output=True)

        assert results3[0]["example_text"] == "test2"

        print("----- All tests passed. -----")
    
    except:
        print("something goofed")
        raise
    
    finally:
        print("Dropping test table.")
        cur.execute(f"drop table {table_attributes['name']};")
        con.close()

if __name__ == "__main__":
    main()