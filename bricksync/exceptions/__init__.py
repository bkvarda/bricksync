class NotIcebergTableError(Exception):
    pass

class ProviderNotFoundError(Exception):
    pass

class TableNotFoundError(Exception):
    pass

class ViewNotFoundError(Exception):
    pass

class UnsupportedTableTypeError(Exception):
    pass
