from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField,
    BooleanType,
    StringType,
    IntegerType,
    FloatType,
    DateType,
    TimeType,
    TimestampType,
    StructType,
    ListType,
)


def json_schema_type_to_pyiceberg_type(json_schema_type_dict: dict) -> str:
    """Convert singer tap json schema type to pyiceberg type."""

    json_schema_type_to_pyiceberg_type_map = {
        "string": StringType(),
        "number": FloatType(),
        "integer": IntegerType(),
        "object": StringType(), # TODO
        "array": StringType(), # TODO
        "boolean": BooleanType(),
        # "null": , # TODO
    }

    type = json_schema_type_dict.get("type")
    format = json_schema_type_dict.get("format", None)

    if type == "string" and format == "time":
        return TimeType()
    if type == "string" and format == "date":
        return DateType()
    if type == "string" and format == "date-time":
        return TimestampType()
    else:
        return json_schema_type_to_pyiceberg_type_map[type]


def build_table_schema(json_schema: dict):
    col_idx = 1
    cols = []

    for k, v in json_schema.items():
        col_idx += 1
        col = NestedField(
            col_idx, k, json_schema_type_to_pyiceberg_type(v), required=False
        )
        cols.append(col)

    pyiceberg_schema = Schema(*cols)

    return pyiceberg_schema
