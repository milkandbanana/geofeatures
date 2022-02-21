import yaml
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType

osmValues = yaml.load(open(r"lib/osmValues.yaml"), Loader=yaml.FullLoader)

lowLvlBuildCnd = (col("k") == "building:levels") & (
    col("v").cast(FloatType()).between(-1, 5)
)

middleLvlBuildCnd = (col("k") == "building:levels") & (
    col("v").cast(FloatType()).between(5, 10)
)

highLvlBuildCnd = (col("k") == "building:levels") & (
    col("v").cast(FloatType()).between(10, 500)
)

oneLvlBuildCnd = (col("k") == "building:levels") & (
    col("v").cast(FloatType()).between(-1, 1.9)
)

otherLvlBuildCnd = (col("k") == "building:levels") & (
    col("v").cast(FloatType()).between(2, 500)
)

allBuildCnd = (
    ((col("k") == "building") & (col("v").isin(osmValues["allBuild"])))
    | ((col("k") == "amenity") & (col("v").isin(osmValues["allBuild"])))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["allBuild"])))
    | ((col("k") == "office") & (col("v").isin(osmValues["allBuild"])))
)

adminBuildCnd = (
    ((col("k") == "building") & (col("v").isin(osmValues["adminBuild"])))
    | ((col("k") == "amenity") & (col("v").isin(osmValues["adminBuild"])))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["adminBuild"])))
    | ((col("k") == "office") & (col("v").isin(osmValues["adminBuild"])))
)

industrialObjCnd = (
    ((col("k") == "amenity") & (col("v").isin(osmValues["industrialObj"])))
    | ((col("k") == "building") & (col("v").isin(osmValues["industrialObj"])))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["industrialObj"])))
)

publicTransportCnd = (
    ((col("k") == "amenity") & (col("v").isin(osmValues["publicTransportObj"])))
    | ((col("k") == "building") & (col("v").isin(osmValues["publicTransportObj"])))
    | (
        (col("osm_type") == "n")
        & (col("k") == "public_transport")
        & (col("v").isin(osmValues["publicTransportObj"]))
    )
    | (
        (col("osm_type") == "n")
        & (col("k") == "railway")
        & (col("v").isin(osmValues["publicTransportObj"]))
    )
    | (
        (col("osm_type") == "n")
        & (col("k") == "highway")
        & (col("v").isin(osmValues["publicTransportObj"]))
    )
    | (
        (col("osm_type") == "n")
        & (col("k") == "type")
        & (col("v").isin("public_transport"))
    )
)

residentObjCnd = (
    ((col("k") == "abutters") & (col("v").isin(osmValues["livingBuild"])))
    | ((col("k") == "building") & (col("v").isin(osmValues["livingBuild"])))
    | ((col("k") == "tourism") & (col("v").isin(osmValues["livingBuild"])))
)

medsObjCnd = ((col("k") == "building") & (col("v").isin(osmValues["medsObj"]))) | (
    (col("k") == "amenity") & (col("v").isin(osmValues["medsObj"]))
)

hotelObjCnd = (
    ((col("k") == "building") & (col("v").isin(osmValues["hotelObj"])))
    | ((col("k") == "leisure") & (col("v").isin(osmValues["hotelObj"])))
    | ((col("k") == "tourism") & (col("v").isin(osmValues["hotelObj"])))
)

entertainObjCnd = (
    ((col("k") == "amenity") & (col("v").isin(osmValues["entertainObj"])))
    | ((col("k") == "building") & (col("v").isin(osmValues["entertainObj"])))
    | ((col("k") == "leisure") & (col("v").isin(osmValues["entertainObj"])))
    | ((col("k") == "tourism") & col("v").isin(osmValues["entertainObj"]))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["entertainObj"])))
)

retailObjCnd = (
    ((col("k") == "amenity") & (col("v").isin(osmValues["retailObj"])))
    | ((col("k") == "building") & (col("v").isin(osmValues["retailObj"])))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["retailObj"])))
)

religiousObjCnd = (
    ((col("k") == "amenity") & (col("v").isin(osmValues["religiousObj"])))
    | ((col("k") == "building") & (col("v").isin(osmValues["religiousObj"])))
    | ((col("k") == "historic") & (col("v").isin(osmValues["religiousObj"])))
    | ((col("k") == "leisure") & (col("v").isin(osmValues["religiousObj"])))
)

histObjCnd = (
    ((col("k") == "amenity") & (col("v").isin(osmValues["histObj"])))
    | ((col("k") == "historic") & (col("v").isin(osmValues["histObj"])))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["histObj"])))
)

parkObjcnd = (
    ((col("k") == "boundary") & (col("v").isin(osmValues["parkObj"])))
    | ((col("k") == "leisure") & (col("v").isin(osmValues["parkObj"])))
    | ((col("k") == "tourism") & (col("v").isin(osmValues["parkObj"])))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["parkObj"])))
)

sportObjCnd = (
    ((col("k") == "amenity") & (col("v").isin(osmValues["sportObj"])))
    | ((col("k") == "building") & (col("v").isin(osmValues["sportObj"])))
    | ((col("k") == "leisure") & (col("v").isin(osmValues["sportObj"])))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["sportObj"])))
)

campObjCnd = (
    ((col("k") == "amenity") & (col("v").isin(osmValues["campObj"])))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["campObj"])))
    | ((col("k") == "leisure") & (col("v").isin(osmValues["campObj"])))
    | ((col("k") == "tourism") & (col("v").isin(osmValues["campObj"])))
)

beachObjCnd = (
    ((col("k") == "leisure") & (col("v").isin(osmValues["beachObj"])))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["beachObj"])))
    | ((col("k") == "amenity") & (col("v").isin(osmValues["beachObj"])))
)

foodObjCnd = ((col("k") == "building") & (col("v").isin(osmValues["foodObj"]))) | (
    (col("k") == "amenity") & (col("v").isin(osmValues["foodObj"]))
)

militaryObjCnd = (
    (col("k") == "building") & (col("v").isin(osmValues["militaryObj"]))
) | ((col("k") == "amenity") & (col("v").isin(osmValues["militaryObj"])))

prisonObjCnd = ((col("k") == "building") & (col("v").isin(osmValues["prisonObj"]))) | (
    (col("k") == "amenity") & (col("v").isin(osmValues["prisonObj"]))
)

parkingObjCnd = (
    ((col("k") == "building") & (col("v").isin(osmValues["parkingObj"])))
    | ((col("k") == "amenity") & (col("v").isin(osmValues["parkingObj"])))
    | ((col("k") == "landuse") & (col("v").isin(osmValues["parkingObj"])))
    | ((col("k") == "leisure") & (col("v").isin(osmValues["parkingObj"])))
)

roadObjCnd = (
    (col("osm_type") == "w")
    & (col("k") == "railway")
    & (col("v").isin(osmValues["roadObj"]))
) | (
    (col("osm_type") == "w")
    & (col("k") == "highway")
    & (col("v").isin(osmValues["roadObj"]))
)
