from typing import Literal, TypedDict

SingerType = Literal["string", "integer", "number", "boolean", "array", "object", "null"]
SingerFormat = Literal["date", "time", "date-time"]


class SingerSchemaType(TypedDict):
    type: SingerType
    format: SingerFormat
    anyOf: "list[SingerSchemaType] | None"


class SingerSchemaProperties(TypedDict):
    id: SingerSchemaType
    name: SingerSchemaType
    updated_at: SingerSchemaType


class SingerSchema(TypedDict):
    type: str
    stream: str
    key_properties: list[str]
    properties: SingerSchemaProperties


class SingerSchemaMessage(TypedDict):
    type: str
    stream: str
    key_properties: list[str]
    schema: SingerSchema
