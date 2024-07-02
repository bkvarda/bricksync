from bricksync.provider.catalog import CatalogProvider
from bricksync.table import Table, View
from sqlglot.dialects.dialect import Dialects

cat = CatalogProvider()

tbl_a = Table("cat.schema.a", "s3://foo/bar")
tbl_b = Table("cat.schema.b", "s3://foo/bar")
tbl_c = Table("schema.c", "s3://foo/bar")
view_a = View("cat.view_schema.a", 
              "select * from cat.schema.a",
              Dialects.DATABRICKS, base_tables=[tbl_a])
view_ab = View("cat.view_schema.ab", 
               "select * from cat.schema.a join cat.schema.b on a.id = b.id",
               Dialects.DATABRICKS, base_tables=[tbl_a, tbl_b])

def test_get_table_parts():
    assert cat.get_fqtn_parts(tbl_a) == ("cat", "schema", "a")
    assert cat.get_fqtn_parts(tbl_b) == ("cat", "schema", "b")
    assert cat.get_fqtn_parts(tbl_c) == ("schema", "c")
    assert cat.get_fqtn_parts(view_a) == ("cat", "view_schema", "a")
    assert cat.get_fqtn_parts(view_ab) == ("cat", "view_schema", "ab")

def test_get_catalog_from_name():
    assert cat.get_catalog_from_name(tbl_a) == "cat"
    assert cat.get_catalog_from_name(tbl_b) == "cat"
    assert cat.get_catalog_from_name(tbl_c) == None
    assert cat.get_catalog_from_name(view_a) == "cat"
    assert cat.get_catalog_from_name(view_ab) == "cat"

def test_get_schema_from_name():
    assert cat.get_schema_from_name(tbl_a) == "schema"
    assert cat.get_schema_from_name(tbl_b) == "schema"
    assert cat.get_schema_from_name(tbl_c) == "schema"
    assert cat.get_schema_from_name(view_a) == "view_schema"
    assert cat.get_schema_from_name(view_ab) == "view_schema"

def test_get_table_from_name():
    assert cat.get_table_from_name(tbl_a) == "a"
    assert cat.get_table_from_name(tbl_b) == "b"
    assert cat.get_table_from_name(tbl_c) == "c"
    assert cat.get_table_from_name(view_a) == "a"
    assert cat.get_table_from_name(view_ab) == "ab"

def test_replace_table_identifiers():
    tbl = Table("cat.schema.a", "s3://foo/bar")
    new_all_table = cat.replace_table_identifiers(tbl_a, "newcat", "newschema", "a_new")
    assert new_all_table.name == "newcat.newschema.a_new"