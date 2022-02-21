from lib.osmConditions import *
from pyspark.sql import DataFrame
from pyspark.sql.types import FloatType

def getOsmObjects(
    osmGeometry: DataFrame, osmTag: DataFrame, osmFeatureSpecification: list
) -> DataFrame:

    selectColumns = ["osm_id", "osm_type", "osm_geom_wkt"] + osmFeatureSpecification

    # get columns with OSM values
    allOsmObjects = (
        osmTag.withColumn("lowLvlBuild", when(lowLvlBuildCnd, 1).otherwise(lit(None)))
        .withColumn("middleLvlBuild", when(middleLvlBuildCnd, 1).otherwise(lit(None)))
        .withColumn("highLvlBuild", when(highLvlBuildCnd, 1).otherwise(lit(None)))
        .withColumn(
            "lvlBuild",
            when(oneLvlBuildCnd, 1)
            .when(otherLvlBuildCnd, col("v").cast(FloatType()))
            .otherwise(lit(None)),
        )
        .withColumn("allBuild", when(allBuildCnd, 1).otherwise(lit(None)))
        .withColumn("adminBuild", when(adminBuildCnd, 1).otherwise(lit(None)))
        .withColumn("industrialObj", when(industrialObjCnd, 1).otherwise(lit(None)))
        .withColumn(
            "publicTransportObj", when(publicTransportCnd, 1).otherwise(lit(None))
        )
        .withColumn("residentObj", when(residentObjCnd, 1).otherwise(lit(None)))
        .withColumn("medsObj", when(medsObjCnd, 1).otherwise(lit(None)))
        .withColumn("hotelObj", when(hotelObjCnd, 1).otherwise(lit(None)))
        .withColumn("entertainObj", when(entertainObjCnd, 1).otherwise(lit(None)))
        .withColumn("retailObj", when(retailObjCnd, 1).otherwise(lit(None)))
        .withColumn("religiousObj", when(religiousObjCnd, 1).otherwise(lit(None)))
        .withColumn("histObj", when(histObjCnd, 1).otherwise(lit(None)))
        .withColumn("parkObj", when(parkObjcnd, 1).otherwise(lit(None)))
        .withColumn("sportObj", when(sportObjCnd, 1).otherwise(lit(None)))
        .withColumn("campObj", when(campObjCnd, 1).otherwise(lit(None)))
        .withColumn("beachObj", when(beachObjCnd, 1).otherwise(lit(None)))
        .withColumn("foodObj", when(foodObjCnd, 1).otherwise(lit(None)))
        .withColumn("militaryObj", when(militaryObjCnd, 1).otherwise(lit(None)))
        .withColumn("prisonObj", when(prisonObjCnd, 1).otherwise(lit(None)))
        .withColumn("parkingObj", when(parkingObjCnd, 1).otherwise(lit(None)))
        .withColumn("roadObj", when(roadObjCnd, 1).otherwise(lit(None)))
    )

    # filtering NULL values (if it do in 1 step: it too long staying at queue)
    allOsmObjects = (
        allOsmObjects.join(osmGeometry, ["osm_id", "osm_type"])
        .withColumnRenamed("geom_wkt", "osm_geom_wkt")
        .select(selectColumns)
    )

    return allOsmObjects
