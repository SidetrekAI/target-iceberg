from typing import cast, Any, List, Tuple, Union
import pyarrow as pa  # type: ignore
from pyarrow import Schema as PyarrowSchema
from pyiceberg.schema import Schema as PyicebergSchema, assign_fresh_schema_ids
from pyiceberg.io.pyarrow import pyarrow_to_schema


# Borrowed from https://github.com/crowemi/target-s3/blob/main/target_s3/formats/format_parquet.py
def singer_to_pyarrow_schema(self, singer_schema: dict) -> PyarrowSchema:
    """Convert singer tap json schema to pyarrow schema."""

    def process_anyof_schema(anyOf: List) -> Tuple[List, Union[str, None]]:
        """This function takes in original array of anyOf's schema detected
        and reduces it to the detected schema, based on rules, right now
        just detects whether it is string or not.
        """
        types, formats = [], []
        for val in anyOf:
            typ = val.get("type")
            if val.get("format"):
                formats.append(val["format"])
            if type(typ) is not list:
                types.append(typ)
            else:
                types.extend(typ)
        types = list(set(types))
        formats = list(set(formats))
        ret_type = []
        if "string" in types:
            ret_type.append("string")
        if "null" in types:
            ret_type.append("null")
        return ret_type, formats[0] if formats else None

    def get_pyarrow_schema_from_array(items: dict, level: int = 0):
        type = cast(list[Any], items.get("type"))
        any_of_types = items.get("anyOf")

        if any_of_types:
            self.logger.info("array with anyof type schema detected.")
            type, _ = process_anyof_schema(anyOf=any_of_types)

        if "string" in type:
            return pa.string()
        elif "integer" in type:
            return pa.int64()
        elif "number" in type:
            return pa.float64()
        elif "boolean" in type:
            return pa.bool_()
        elif "array" in type:
            subitems = cast(dict, items.get("items"))
            return pa.list_(get_pyarrow_schema_from_array(items=subitems, level=level))
        elif "object" in type:
            subproperties = cast(dict, items.get("properties"))
            return pa.struct(get_pyarrow_schema_from_object(properties=subproperties, level=level + 1))
        else:
            return pa.null()

    def get_pyarrow_schema_from_object(properties: dict, level: int = 0):
        """
        Returns schema for an object.
        """
        fields = []
        field_id = 0
        for key, val in properties.items():
            field_id = 1  # Just set it to 1 here - it'll be overwritten to handle nested schema
            field_metadata = {"PARQUET:field_id": f"{field_id}"}

            if "type" in val.keys():
                type = val["type"]
                format = val.get("format")
            elif "anyOf" in val.keys():
                type, format = process_anyof_schema(val["anyOf"])
            else:
                self.logger.warning("type information not given")
                type = ["string", "null"]

            if "integer" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.int64(), nullable=nullable, metadata=field_metadata))
            elif "number" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.float64(), nullable=nullable, metadata=field_metadata))
            elif "boolean" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.bool_(), nullable=nullable, metadata=field_metadata))
            elif "string" in type:
                nullable = "null" in type
                if format and level == 0:
                    # this is done to handle explicit datetime conversion
                    # which happens only at level 1 of a record
                    if format == "date":
                        fields.append(pa.field(key, pa.date64(), nullable=nullable, metadata=field_metadata))
                    elif format == "time":
                        fields.append(pa.field(key, pa.time64(), nullable=nullable, metadata=field_metadata))
                    else:
                        fields.append(
                            pa.field(
                                key,
                                pa.timestamp("us", tz="UTC"),
                                metadata=field_metadata,
                            )
                        )
                else:
                    fields.append(pa.field(key, pa.string(), nullable=nullable, metadata=field_metadata))
            elif "array" in type:
                nullable = "null" in type
                items = val.get("items")
                if items:
                    item_type = get_pyarrow_schema_from_array(items=items, level=level)
                    if item_type == pa.null():
                        self.logger.warn(
                            f"""key: {key} is defined as list of null, while this would be
                                correct for list of all null but it is better to define
                                exact item types for the list, if not null."""
                        )
                    fields.append(pa.field(key, pa.list_(item_type), nullable=nullable, metadata=field_metadata))
                else:
                    self.logger.warn(
                        f"""key: {key} is defined as list of null, while this would be
                            correct for list of all null but it is better to define
                            exact item types for the list, if not null."""
                    )
                    fields.append(pa.field(key, pa.list_(pa.null()), nullable=nullable, metadata=field_metadata))
            elif "object" in type:
                nullable = "null" in type
                prop = val.get("properties")
                inner_fields = get_pyarrow_schema_from_object(properties=prop, level=level + 1)
                if not inner_fields:
                    self.logger.warn(
                        f"""key: {key} has no fields defined, this may cause
                            saving parquet failure as parquet doesn't support
                            empty/null complex types [array, structs] """
                    )
                fields.append(pa.field(key, pa.struct(inner_fields), nullable=nullable, metadata=field_metadata))

        return fields

    properties = singer_schema["properties"]
    pyarrow_schema = pa.schema(get_pyarrow_schema_from_object(properties=properties))

    return pyarrow_schema


def pyarrow_to_pyiceberg_schema(self, pyarrow_schema: PyarrowSchema) -> PyicebergSchema:
    """Convert pyarrow schema to pyiceberg schema."""
    pyiceberg_schema = pyarrow_to_schema(pyarrow_schema)
    self.logger.info(f"PyIceberg Schema: {pyiceberg_schema}")

    # Overwrite the default field_ids of 1 to unique ids (this ensures the nested fields are correctly handled)
    pyiceberg_schema_with_field_ids = assign_fresh_schema_ids(pyiceberg_schema)

    return pyiceberg_schema_with_field_ids
