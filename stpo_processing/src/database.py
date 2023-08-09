import logging
import os

from dotenv import load_dotenv
import psycopg2
import psycopg2.extensions
from psycopg2 import sql

from .constants import DEBUG, SQL_INDENT

logger = logging.getLogger(__name__)
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

# Load environment variables from .env
load_dotenv()


def get_database_credentials() -> dict:
    try:
        logger.debug("Getting database credentials.")
        database_credentials = {
            "user": os.getenv("STPODB_USER"),
            "password": os.getenv("STPODB_PASSWORD"),
            "host": os.getenv("STPODB_HOST"),
            "port": os.getenv("STPODB_PORT"),
            "database": os.getenv("STPODB_DATABASE"),
            "sslmode": os.getenv("STPODB_SSLMODE"),
        }
    except Exception as e:
        logger.warning("ERROR GETTING DATABASE CREDENTIALS:", e)
        raise

    return database_credentials


def get_connection_and_cursor():
    try:
        logger.debug("Getting connection and cursor.")
        db_creds = get_database_credentials()
        logger.debug("Database credentials retrieved.")
        connection = psycopg2.connect(**db_creds)
        logger.debug("Connection established.")
        cursor = connection.cursor(cursor_factory=STPOCursor)
        logger.debug("Cursor created.")
        connection.autocommit = True
    except Exception as e:
        logger.warning("ERROR GETTING CONNECTION AND CURSOR:", e)
        raise RuntimeError(e)

    return connection, cursor


class STPOCursor(psycopg2.extensions.cursor):
    def execute(self, sql, args=None):
        try:
            psycopg2.extensions.cursor.execute(self, sql, args)
        except Exception as exc:
            print(f"{exc.__class__.__name__} {exc}")
            raise

    def fetchone(self):
        try:
            return psycopg2.extensions.cursor.fetchone(self)
        except Exception as exc:
            print(f"{exc.__class__.__name__} {exc}")
            raise

    def fetchmany(self, size):
        try:
            return psycopg2.extensions.cursor.fetchmany(self, size)
        except Exception as exc:
            print(f"{exc.__class__.__name__} {exc}")
            raise

    def fetchall(self):
        try:
            return psycopg2.extensions.cursor.fetchall(self)
        except Exception as exc:
            print(f"{exc.__class__.__name__} {exc}")
            raise

    def _complete_attributes(
        self, attributes: dict, attributes_reference: list
    ) -> dict:
        for attribute in attributes_reference:
            if attribute not in attributes.keys():
                attributes[attribute] = ""

        return attributes

    def create_table(self, context, table_attributes: dict, verbose=False) -> None:
        """
        Table attributes:
            {
                "name": "<table_name>",
                "temp": False,
                "is_if_not_exists": True,
                "columns": [
                    {
                        "name": "created_at <column_name>",
                        "data_type": "TIMESTAMP WITHOUT TIME ZONE<data_type>",
                        "default": "(NOW() AT TIME ZONE 'UTC') <value>",
                        "is_null": False,
                        "constraint": ""

                    },
                    {...},
                    ...
                ]
            }
        """

        table_attributes_reference = ["name", "temp", "is_if_not_exists"]
        column_attributes_reference = [
            "name",
            "data_type",
            "default",
            "is_null",
            "constraint",
        ]

        table_attributes = self._complete_attributes(
            table_attributes, table_attributes_reference
        )

        # Build create table command from attributes
        create_table_text = "CREATE "
        if table_attributes["temp"]:
            create_table_text += "TEMPORARY "
        create_table_text += "TABLE "
        if table_attributes["is_if_not_exists"]:
            create_table_text += "IF NOT EXISTS"
        create_table_text += "\n" + (" " * SQL_INDENT)
        create_table_text += "{table_name} ({field_definitions});"

        create_table_sql = sql.SQL(create_table_text)

        table_name_idn = sql.Identifier(table_attributes["name"])

        if not table_attributes["columns"]:
            print(f"Table {table_attributes['name']} has no columns!")
            raise

        column_types = []
        for col_attr in table_attributes["columns"]:
            if col_attr:
                col_attr = self._complete_attributes(
                    col_attr, column_attributes_reference
                )

                col_name = sql.Identifier(col_attr["name"].lower())
                col_text = f" {col_attr['data_type'].upper()}"
                if col_attr["default"]:
                    col_text += f" DEFAULT {col_attr['default'].upper()}"
                if not col_attr["is_null"]:
                    col_text += " NOT NULL"
                if col_attr["constraint"]:
                    col_text += f" {col_attr['constraint'].upper()}"
                column_types.append(col_name.as_string(context) + col_text)

        field_attributes = sql.SQL(", ".join(column_types))

        query = create_table_sql.format(
            table_name=table_name_idn, field_definitions=field_attributes
        )

        if verbose:
            print(query.as_string(context))

        self.execute(query)

    def insert_into_table(self, context, table_row: dict, verbose=False) -> None:
        """
        table_row = {
            "table_name": "<table_name>",
            "column_data": [
                {
                    "name": "<column_name>",
                    "value": <column_value>
                },
                {...},
                ...
            ]
        }
        """
        insert_statement = sql.SQL(
            "INSERT INTO {table_name} ({col_names}) VALUES ({col_values});"
        )

        table_name = sql.Identifier(table_row["table_name"])
        col_names = [sql.Identifier(col["name"]) for col in table_row["column_data"]]
        col_values = [col["value"] for col in table_row["column_data"]]

        query = insert_statement.format(
            table_name=table_name,
            col_names=sql.SQL(", ").join(col_names),
            col_values=sql.SQL(", ").join(
                sql.Placeholder() * len(table_row["column_data"])
            ),
        )

        if verbose:
            print(query.as_string(context))

        self.execute(query, col_values)

    def _output_tuples_to_dicitonaries(self, columns, output_rows):
        """For use with select_from_table."""
        dictionaries = []
        for output_row in output_rows:
            output_record = {}
            for i in range(len(columns)):
                output_record[columns[i]] = output_row[i]
            dictionaries.append(output_record)
        return dictionaries

    def select_from_table(
        self, context, select_attrs, dict_output=False, verbose=False
    ):
        """
        select_attrs = {
            "table_name": "<table_name>",
            "columns": ["<col_name>", ...],
            "where" <optional>: [
                {
                    "column": "<col_name>",
                    "operator": "<comparison_operator>",
                    "value": <comparison_value>
                }, {...}, ...
            ]
            "limit" <optional>: <int>
        }
        """
        if "columns" in select_attrs.keys():
            query = sql.SQL("SELECT {columns} FROM {table_name}").format(
                columns=sql.SQL(", ").join(
                    [sql.Identifier(col) for col in select_attrs["columns"]]
                ),
                table_name=sql.Identifier(select_attrs["table_name"]),
            )
        else:
            query_text = f"SELECT {select_attrs['text']} "
            query_text += "FROM {table_name}"
            query = sql.SQL(query_text).format(
                table_name=sql.Identifier(select_attrs["table_name"])
            )
        execution_values = []

        if "where" in select_attrs.keys():
            query += sql.SQL(" WHERE ")
            where_conditions = []
            for where_element in select_attrs["where"]:
                if "text" in where_element.keys():
                    # Don't use this if you can avoid it
                    where_conditions.append(sql.SQL(where_element["text"]))
                else:
                    where_text = "{where_column} "
                    where_text += where_element["operator"]
                    where_condition = sql.SQL(where_text).format(
                        where_column=sql.Identifier(where_element["column"])
                    )
                    where_condition += sql.SQL(" %s")
                    where_conditions.append(where_condition)
                    execution_values.append(where_element["value"])

            query += sql.SQL(" AND").join(where_conditions)

        if "limit" in select_attrs.keys():
            query += sql.SQL(f" LIMIT {select_attrs['limit']}")

        query += sql.SQL(";")

        if verbose:
            print(query.as_string(context))

        self.execute(query, execution_values)

        results = self.fetchall()

        if dict_output:
            results = self._output_tuples_to_dicitonaries(
                select_attrs["columns"], results
            )

        # if verbose:
        #     print(results)

        return results
