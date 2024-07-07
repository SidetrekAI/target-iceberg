from typing import cast, Any, List, Tuple, Union
import pyarrow as pa  # type: ignore
from pyarrow import Schema as PyarrowSchema, Field as PyarrowField
from pyiceberg.schema import Schema as PyicebergSchema
from pyiceberg.io.pyarrow import pyarrow_to_schema


# Borrowed from https://github.com/crowemi/target-s3/blob/main/target_s3/formats/format_parquet.py
def singer_to_pyarrow_schema_without_field_ids(self, singer_schema: dict) -> PyarrowSchema:
    """Convert singer tap json schema to pyarrow schema."""

    def process_anyof_schema(anyOf: List) -> Tuple[List[str], Union[str, None]]:
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

    def get_pyarrow_schema_from_object(properties: dict, level: int = 0) -> List[pa.Field]:
        fields = []
        for key, val in properties.items():
            types, format = process_anyof_schema(val.get('anyOf', [])) if 'anyOf' in val else (val.get('type', []), val.get('format'))
            nullable = 'null' in types
            field_type = determine_field_type(key, types, format, val, level)
            if field_type:
                fields.append(pa.field(key, field_type, nullable=nullable, metadata=val.get('metadata', {})))
        return fields

    def determine_field_type(key: str, types: List[str], format: str, val: dict, level: int) -> pa.DataType:
        if "object" in types:
            return pa.struct(get_pyarrow_schema_from_object(val.get('properties', {}), level + 1))
        elif "array" in types:
            return pa.list_(get_pyarrow_schema_from_array(val.get('items', {}), level))
        elif "integer" in types:
            return pa.int64()
        elif "number" in types:
            return pa.float64()
        elif "boolean" in types:
            return pa.bool_()
        elif "string" in types:
            if format and level == 0:
                return pa.timestamp('us', tz='UTC') if format not in {"date", "time"} else pa.date64() if format == "date" else pa.time64()
            return pa.string()
        return pa.null()

    properties = singer_schema["properties"]
    pyarrow_schema = pa.schema(get_pyarrow_schema_from_object(properties=properties))

    return pyarrow_schema


def assign_pyarrow_field_ids(pa_fields: List[pa.Field], field_id: int = 0) -> Tuple[List[pa.Field], int]:
    """Assigns unique field IDs to the PyArrow schema fields."""
    new_fields = []
    for field in pa_fields:
        nested_pa_fields, field_id = assign_pyarrow_field_ids([field.type.field(i) for i in range(field.type.num_fields)], field_id)
        new_fields.append(pa.field(field.name, pa.struct(nested_pa_fields), nullable=field.nullable, metadata=field.metadata))
        # if isinstance(field.type, pa.StructType):
        #     nested_pa_fields, field_id = assign_pyarrow_field_ids([field.type.field(i) for i in range(field.type.num_fields)], field_id)
        #     new_fields.append(pa.field(field.name, pa.struct(nested_pa_fields), nullable=field.nullable, metadata=field.metadata))
        # else:
        #     field_id += 1
        #     field_with_metadata = field.with_metadata({**field.metadata, "PARQUET:field_id": str(field_id)})
        #     new_fields.append(field_with_metadata)
    return new_fields, field_id


def singer_to_pyarrow_schema(self, singer_schema: dict) -> PyarrowSchema:
    """Converts a Singer JSON schema to a PyArrow schema with field IDs."""
    pa_schema = singer_to_pyarrow_schema_without_field_ids(self, singer_schema)
    pa_fields_with_field_ids, _ = assign_pyarrow_field_ids(pa_schema)
    return pa.schema(pa_fields_with_field_ids)


def pyarrow_to_pyiceberg_schema(self, pa_schema: PyarrowSchema) -> PyicebergSchema:
    """Convert pyarrow schema to pyiceberg schema."""
    pyiceberg_schema = pyarrow_to_schema(pa_schema)
    return pyiceberg_schema




