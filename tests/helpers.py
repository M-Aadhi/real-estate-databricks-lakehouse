"""Test helpers."""
from decimal import Decimal

from pyspark.sql import types as T


def make_df(spark, rows, schema: str):
    """createDataFrame that accepts plain ints/floats for decimal and double columns."""
    struct = spark.createDataFrame([], schema).schema
    def convert(value, dtype):
        if value is None:
            return None
        if isinstance(dtype, T.DecimalType):
            return Decimal(str(value))
        if isinstance(dtype, T.DoubleType):
            return float(value)
        return value
    fixed = [tuple(convert(v, f.dataType) for v, f in zip(row, struct.fields, strict=True)) for row in rows]
    return spark.createDataFrame(fixed, struct)
