from pyspark.sql.functions import *
from pyspark.sql import DataFrame

def getLocality(
    locality: DataFrame, localityGeometry: DataFrame, localityDistrict: DataFrame
) -> DataFrame:

    districtAlias = "district"
    localityAlias = "locality"

    selectColumns = [
        "locality_id",
        f"{districtAlias}.population as {districtAlias}_population",
        f"{localityAlias}.population as {localityAlias}_population",
        "locality_area",
        "district_area",
        "dist_to_region_capital",
        "dist_to_district_center",
        "geometry",
    ]

    localityObj = (
        locality.alias(f"{localityAlias}")
        .join(localityDistrict.alias(f"{districtAlias}"), ["district_id"], "inner")
        .join(localityGeometry, ["poly_id"], "inner")
        .withColumnRenamed("polygon", "geometry")
        .selectExpr(selectColumns)
    )

    return localityObj
