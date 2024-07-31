from bricksync.table import Table, View, DeltaTable, IcebergTable, UniformIcebergInfo
from pytest import fixture

@fixture
def delta_table():
    return DeltaTable(name="cat.schema.a", 
                      storage_location="s3://foo/bar", 
                      delta_properties={"delta": "properties"},
                      uniform_iceberg_info=None)

@fixture
def iceberg_table():
    return IcebergTable(name="cat.schema.b", 
                        storage_location="s3://foo/bar", 
                        iceberg_metadata_location="s3://foo/bar/metadata")

@fixture
def uniform_table():
    return DeltaTable(name="cat.schema.c", 
                      storage_location="s3://foo/bar", 
                      delta_properties={"delta": "properties"},
                      uniform_iceberg_info=UniformIcebergInfo(
                          metadata_location="s3://foo/bar/metadata",
                          converted_delta_version=8,
                          converted_delta_timestamp="2021-06-01T00:00:00Z"
                      ))

@fixture
def view():
    return View(name="cat.view_schema.a", 
                view_definition="select * from cat.schema.a",
                dialect="databricks",
                base_tables=[DeltaTable(name="cat.schema.a", 
                                        storage_location="s3://foo/bar",
                                        delta_properties={"delta": "properties"},
                                        uniform_iceberg_info=None)])


def test_delta_table(delta_table):
    assert delta_table.name == "cat.schema.a"
    assert delta_table.storage_location == "s3://foo/bar"
    assert delta_table.delta_properties == {"delta": "properties"}
    assert not delta_table.is_iceberg()
    assert delta_table.is_delta()

def test_iceberg_table(iceberg_table):
    assert iceberg_table.name == "cat.schema.b"
    assert iceberg_table.storage_location == "s3://foo/bar"
    assert iceberg_table.iceberg_metadata_location == "s3://foo/bar/metadata"
    assert iceberg_table.is_iceberg()
    assert not iceberg_table.is_delta()

def test_view(view):
    view.set_name("cat.view_schema.b")
    assert view.name == "cat.view_schema.b"
    view.set_view_definition("select * from cat.schema.b")
    assert view.view_definition == "select * from cat.schema.b"
    assert not view.is_table()
    assert view.is_view()


