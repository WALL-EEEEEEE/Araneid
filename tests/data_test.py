from unittest import TestCase
from araneid.data.item import Field, Type, Item
from araneid.data.check.schema import ItemSchema, schemas, register
from araneid.data.check import Is 

class TestItem(Item):
    name = Field(type=Type.STRING, require=True)

@register
class TestItemSchema(ItemSchema):
    name = Is.NotEmpty | Is.String


def test_item_create():
    item = TestItem()
    item['name'] = 'hello'
    item2 = TestItem()
    name_field = Field(name='name', type=Type.STRING, require=True)
    name_field.value = 'hello'
    name_field2 = Field(name='name', type=Type.STRING, require=True)
    assert name_field in item.fields
    assert name_field2 in item2.fields

def test_item_assign():
    v = 'hello'
    item = TestItem()
    item['name'] = v
    assert item['name'].value == v

def test_schema_create():
    itemSchema = TestItemSchema()
    name_checker = Is.NotEmpty | Is.String
    assert name_checker in itemSchema.rules

def test_schema_check_fail():
    check_result = f'{TestItem.__qualname__} {{ name: must be not empty }}'
    item = TestItem()
    schema = TestItemSchema()
    error = schema.check(item)
    assert check_result == str(error)

def test_schema_check_pass():
    item = TestItem()
    item['name'] = 'hello'
    schema = TestItemSchema()
    error = schema.check(item)
    assert error is None

def test_register_schema_fail():
    item = TestItem()
    error = schemas.check(item)
    check_result = f'{TestItem.__qualname__} {{ name: must be not empty }}'
    assert check_result == str(error)

def test_register_schema_pass():
    item = TestItem()
    item['name'] = 'hello'
    error = schemas.check(item)
    assert error is None
 
