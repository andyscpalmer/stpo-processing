DEBUG = True

SQL_INDENT = 4

RAW_POSTS_TABLE_MODEL = {
    "name": "raw_post_data",
    "temp": False,
    "is_if_not_exists": True,
    "columns": [
        {
            "name": "id",
            "data_type": "serial",
            "is_null": False,
            "constraint": "primary key",
        },
        {"name": "raw_post_text", "data_type": "text", "is_null": False},
        {
            "name": "created_at",
            "data_type": "timestamp without time zone",
            "default": "(now() at time zone 'utc')",
            "is_null": False,
        },
    ],
}

STPO_MAP_MODEL = {
    "name": "stpo_map",
    "temp": False,
    "is_if_not_exists": True,
    "columns": [
        {
            "name": "id",
            "data_type": "serial",
            "is_null": False,
            "constraint": "primary key",
        },
        {
            "name": "created_at",
            "data_type": "timestamp without time zone",
            "default": "(now() at time zone 'utc')",
            "is_null": False,
        },
        {
            "name": "stpo_snapshot",
            "data_type": "jsonb",
            "is_null": False,
        },
        {"name": "snapshot_interval", "data_type": "interval", "is_null": False},
    ],
}

LOGGING_MODEL = {
    "name": "logs",
    "temp": False,
    "is_if_not_exists": True,
    "columns": [
        {
            "name": "id",
            "data_type": "serial",
            "is_null": False,
            "constraint": "primary key",
        },
        {"name": "log_level", "data_type": "integer", "is_null": True},
        {"name": "log_levelname", "data_type": "text", "is_null": False},
        {"name": "log", "data_type": "text", "is_null": False},
        {
            "name": "created_at",
            "data_type": "timestamp without time zone",
            "default": "(now() at time zone 'utc')",
            "is_null": False,
        },
        {"name": "created_by", "data_type": "text", "is_null": False},
    ],
}
