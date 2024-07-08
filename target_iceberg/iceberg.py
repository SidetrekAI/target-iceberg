from typing import cast, Any, List, Tuple, Union
import pyarrow as pa  # type: ignore
from pyarrow import Schema as PyarrowSchema, Field as PyarrowField
from pyiceberg.schema import Schema as PyicebergSchema
from pyiceberg.io.pyarrow import pyarrow_to_schema


# Borrowed from https://github.com/crowemi/target-s3/blob/main/target_s3/formats/format_parquet.py
def singer_to_pyarrow_schema_without_field_ids(self, singer_schema: dict) -> PyarrowSchema:
    """Convert singer tap json schema to pyarrow schema."""

    def process_anyof_schema(anyOf: List) -> Tuple[List[str], Union[str, None]]:
        """Processes 'anyOf' schema entries to determine the applicable types and formats."""
        types, formats = set(), set()
        for val in anyOf:
            typ = val.get("type", [])
            formats.update(val.get("format", []))
            if isinstance(typ, list):
                types.update(typ)
            else:
                types.add(typ)
        ret_type = ['string'] if 'string' in types else list(types)
        if 'null' in types:
            ret_type.append('null')
        return ret_type, formats.pop() if formats else None

    def get_pyarrow_schema_from_array(items: dict, level: int = 0) -> pa.DataType:
        """Returns the PyArrow schema for array items."""
        types, format = process_anyof_schema(items.get('anyOf', [])) if 'anyOf' in items else (items.get('type', []), None)
        
        type_mapping = {
            "string": pa.string(),
            "integer": pa.int64(),
            "number": pa.float64(),
            "boolean": pa.bool_(),
            "array": pa.list_(get_pyarrow_schema_from_array(items.get("items", {}), level)),
            "object": pa.struct(get_pyarrow_schema_from_object(items.get("properties", {}), level + 1))
        }
        for typ in types:
            if typ in type_mapping:
                return type_mapping[typ]
        return pa.null()

    def get_pyarrow_schema_from_object(properties: dict, level: int = 0):
        """Returns schema for an object."""
        self.logger.info(f"********** properties: {properties} at level: {level}**********")

        fields = []

        if not properties:
            self.logger.warning(f"**********No properties found for the object at level: {level}**********")
            # If the properties dictionary is empty, return a string field
            fields.append(pa.field('unknown', pa.string(), nullable=True))
            return fields

        for key, val in properties.items():
            if "type" in val.keys():
                type = val["type"]
                format = val.get("format")
            elif "anyOf" in val.keys():
                type, format = process_anyof_schema(val["anyOf"])
            else:
                self.logger.warning("type information not given")
                type = ["string", "null"]

            if "object" in type:
                nullable = "null" in type
                prop = val.get("properties")
                inner_fields = get_pyarrow_schema_from_object(properties=prop, level=level + 1)
                if not inner_fields:
                    self.logger.warn(
                        f"""key: {key} has no fields defined, this may cause
                            saving parquet failure as parquet doesn't support
                            empty/null complex types [array, structs]. Converting to string."""
                    )
                    fields.append(pa.field(key, pa.string(), nullable=nullable))
                else:
                    fields.append(pa.field(key, pa.struct(inner_fields), nullable=nullable))
            elif "integer" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.int64(), nullable=nullable))
            elif "number" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.float64(), nullable=nullable))
            elif "boolean" in type:
                nullable = "null" in type
                fields.append(pa.field(key, pa.bool_(), nullable=nullable))
            elif "string" in type:
                nullable = "null" in type
                if format and level == 0:
                    # this is done to handle explicit datetime conversion
                    # which happens only at level 1 of a record
                    if format == "date":
                        fields.append(pa.field(key, pa.date64(), nullable=nullable))
                    elif format == "time":
                        fields.append(pa.field(key, pa.time64(), nullable=nullable))
                    else:
                        fields.append(pa.field(key, pa.timestamp("us", tz="UTC"), nullable=nullable))
                else:
                    fields.append(pa.field(key, pa.string(), nullable=nullable))
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
                    fields.append(pa.field(key, pa.list_(item_type), nullable=nullable))
                else:
                    self.logger.warn(
                        f"""key: {key} is defined as list of null, while this would be
                            correct for list of all null but it is better to define
                            exact item types for the list, if not null."""
                    )
                    fields.append(pa.field(key, pa.list_(pa.null()), nullable=nullable))
        self.logger.info(f"********** fields: {fields} at level: {level}**********")
        return fields

    properties = singer_schema["properties"]
    pyarrow_schema = pa.schema(get_pyarrow_schema_from_object(properties=properties))

    return pyarrow_schema


def assign_pyarrow_field_ids(pa_fields: List[pa.Field], field_id: int = 0) -> Tuple[List[pa.Field], int]:
    """Assigns unique field IDs to the PyArrow schema fields."""
    new_fields = []
    for field in pa_fields:
        if isinstance(field.type, pa.StructType):
            nested_pa_fields, field_id = assign_pyarrow_field_ids(list(field.type), field_id)
            new_fields.append(pa.field(field.name, pa.struct(nested_pa_fields), nullable=field.nullable, metadata=field.metadata))
        else:
            field_id += 1
            # Handle NoneType metadata
            metadata = field.metadata or {}
            field_with_metadata = field.with_metadata({**metadata, "PARQUET:field_id": str(field_id)})
            new_fields.append(field_with_metadata)
    return new_fields, field_id




def singer_to_pyarrow_schema(self, singer_schema: dict) -> PyarrowSchema:
    """Convert singer tap json schema to pyarrow schema."""
    pa_schema = singer_to_pyarrow_schema_without_field_ids(self, singer_schema)
    self.logger.info(f"********** pa_schema: {pa_schema} **********")
    
    # Extract fields from pa_schema
    pa_fields = pa_schema
    
    # Pass pa_fields to assign_pyarrow_field_ids
    pa_fields_with_field_ids, _ = assign_pyarrow_field_ids(pa_fields)
    
    return pa.schema(pa_fields_with_field_ids)


def pyarrow_to_pyiceberg_schema(self, pa_schema: PyarrowSchema) -> PyicebergSchema:
    """Convert pyarrow schema to pyiceberg schema."""
    pyiceberg_schema = pyarrow_to_schema(pa_schema)
    return pyiceberg_schema




