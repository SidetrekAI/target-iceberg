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


def singer_schema_to_pyiceberg_schema(self, singer_schema: dict) -> Schema:
    """Convert singer tap json schema to pyiceberg schema."""

    def get_pyiceberg_fields_from_array(
        field_idx: int, field_name: str, items: dict, level: int = 0
    ) -> list:
        type = items.get("type")

        def get_nested_field(_field_type, required=False):
            return NestedField(
                field_id=field_idx,
                name=field_name,
                field_type=_field_type,
                required=required,
            )

        if "string" in type:
            if format == "time":
                return get_nested_field(TimeType())
            elif format == "date":
                return get_nested_field(DateType())
            elif format == "date-time":
                return get_nested_field(TimestampType())
            else:
                return get_nested_field(StringType())
        elif "integer" in type:
            return get_nested_field(IntegerType())
        elif "number" in type:
            return get_nested_field(FloatType())
        elif "boolean" in type:
            return get_nested_field(BooleanType())
        elif "array" in type:
            inner_fields = get_pyiceberg_fields_from_array(
                field_idx, field_name, items.get("items"), level
            )
            return get_nested_field(ListType(*inner_fields))
        elif "object" in type:
            inner_fields = get_pyiceberg_fields_from_object(
                items.get("properties"), level + 1
            )
            return get_nested_field(StructType(*inner_fields))
        else:
            # Fallback to string
            return get_nested_field(StringType())

    def get_pyiceberg_fields_from_object(properties: dict, level: int = 0) -> list:
        fields = []
        field_idx = 1
        for field_name, val in properties.items():
            field_idx += 1
            type = val.get("type")  # this is a `list`!
            format = val.get("format")

            def get_nested_field(_field_type, required=False):
                return NestedField(
                    field_id=field_idx,
                    name=field_name,
                    field_type=_field_type,
                    required=required,
                )

            if "string" in type:
                if format == "time":
                    return fields.append(get_nested_field(TimeType()))
                elif format == "date":
                    return fields.append(get_nested_field(DateType()))
                elif format == "date-time":
                    return fields.append(get_nested_field(TimestampType()))
                else:
                    return fields.append(get_nested_field(StringType()))
            elif "integer" in type:
                return fields.append(get_nested_field(IntegerType()))
            elif "number" in type:
                return fields.append(get_nested_field(FloatType()))
            elif "boolean" in type:
                return fields.append(get_nested_field(BooleanType()))
            elif "array" in type:
                items = val.get("items")
                if items:
                    inner_fields = get_pyiceberg_fields_from_array(
                        field_idx, field_name, items, level
                    )
                else:
                    inner_fields = []
                return fields.append(get_nested_field(ListType(*inner_fields)))
            elif "object" in type:
                inner_fields = get_pyiceberg_fields_from_object(
                    val.get("properties"), level + 1
                )
                return fields.append(get_nested_field(StructType(*inner_fields)))
            else:
                # Fallback to string
                return fields.append(get_nested_field(StringType()))

        return fields

    return Schema(*get_pyiceberg_fields_from_object(singer_schema["properties"]))
