from pyspark.sql.functions import *
from pyspark.sql import DataFrame

def getFias(fiasGeometry: DataFrame, fiasFeatures: DataFrame) -> DataFrame:

    selectColumns = [
        "houseguid",
        "flatnumber as flats",
        "roomnumber as rooms",
        "flattype",
        "geometry",
    ]

    fiasObj = fiasFeatures.join(fiasGeometry, ["houseguid"], "inner").selectExpr(
        selectColumns
    )

    return fiasObj
