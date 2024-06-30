"""Contains OracleEngineAdapter."""

from __future__ import annotations

import typing as t

import numpy as np
import pandas as pd
from pandas.api.types import is_datetime64_any_dtype  # type: ignore
from sqlglot import exp

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.base import EngineAdapterWithIndexSupport
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    InsertOverwriteWithMergeMixin,
    PandasNativeFetchDFSupportMixin,
    VarcharSizeWorkaroundMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    SourceQuery,
    set_catalog,
)
from sqlmesh.core.schema_diff import SchemaDiffer

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, Query


@set_catalog()
class OracleEngineAdapter(
    EngineAdapterWithIndexSupport,
    PandasNativeFetchDFSupportMixin,
    InsertOverwriteWithMergeMixin,
    GetCurrentCatalogFromFunctionMixin,
    VarcharSizeWorkaroundMixin,
):
    DIALECT: str = "oracle"
    SUPPORTS_TUPLE_IN = False
    SUPPORTS_MATERIALIZED_VIEWS = True
    CATALOG_SUPPORT = CatalogSupport.UNSUPPORTED
    # CURRENT_CATALOG_EXPRESSION = exp.func("db_name")
    COMMENT_CREATION_TABLE = CommentCreationTable.UNSUPPORTED
    COMMENT_CREATION_VIEW = CommentCreationView.UNSUPPORTED
    SUPPORTS_REPLACE_TABLE = False
    SCHEMA_DIFFER = SchemaDiffer(
        parameterized_type_defaults={
            exp.DataType.build("NUMBER", dialect=DIALECT).this: [(40, 40), (0,)],
            exp.DataType.build("BLOB", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("CLOB", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("VARCHAR2", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("NCHAR", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("NVARCHAR2", dialect=DIALECT).this: [(1,)],
            exp.DataType.build("TIMESTAMP", dialect=DIALECT).this: [(7,)],
            exp.DataType.build("DATE", dialect=DIALECT).this: [(7,)],
            exp.DataType.build("INTERVAL", dialect=DIALECT).this: [(7,)],
        },
        max_parameter_length={
            exp.DataType.build("BLOB", dialect=DIALECT).this: 2147483647,  # 2 GB
            exp.DataType.build("VARCHAR2", dialect=DIALECT).this: 32767,
            exp.DataType.build("NVARCHAR", dialect=DIALECT).this: 2147483647,
        },
    )

    def ping(self) -> None:
        self._connection_pool.get().ping()
    def _rename_table(
        self,
        old_table_name: TableName,
        new_table_name: TableName,
    ) -> None:
        # The function that renames tables in MSSQL takes string literals as arguments instead of identifiers,
        # so we shouldn't quote the identifiers.
        self.execute(exp.rename_table(old_table_name, new_table_name), quote_identifiers=False)
