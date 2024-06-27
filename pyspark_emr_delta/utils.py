from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def mul_double(val: int) -> IntegerType:
    return val * 2


mul_double_udf = udf(lambda x: mul_double(x), IntegerType())
