import yaml
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

projections = yaml.load(open(r"lib/projections.yaml"), Loader=yaml.FullLoader)


def toLocalCrs(
    pointLonCol: str,
    pointLatCol: str,
    localProjectionCol: str,
    localCrsName: str,
    geometry: str,
    data: DataFrame,
) -> DataFrame:

    localCrsCnd = (
        "CASE "
        + " ".join(
            list(
                map(
                    lambda proj: f""" WHEN {pointLonCol} >= {proj[1]} AND \
                {pointLonCol} < {proj[2]} AND \
                {pointLatCol} >= {proj[3]} AND \
                {pointLatCol} < {proj[4]} THEN '{proj[0]}' """,
                    projections,
                )
            )
        )
        + "ELSE 'epsg:3857' END"
    )

    data = data.withColumn(f"{localCrsName}", expr(localCrsCnd)).withColumn(
        f"{localProjectionCol}",
        expr(
            f"ST_AsText(ST_Transform(ST_GeomFromWKT({geometry}), 'epsg:4326', {localCrsName}))"
        ),
    )

    return data


def bufferization(
    data: DataFrame, geometry: str, localCrsName: str, buffer: list
) -> DataFrame:

    dataBuffer = data

    for currentBuffer in buffer:
        currentBufferName = str(currentBuffer).replace(".", "")
        dataBuffer = dataBuffer.withColumn(
            f"buffer_{currentBufferName}",
            expr(
                f"ST_AsText(ST_Transform(ST_Buffer(ST_GeomFromWKT({geometry}), \
                                                                                     {currentBuffer}), \
                                                                           {localCrsName}, 'epsg:4326'))"
            ),
        )
    return dataBuffer


def saveTableOverwritePartition(
    data: DataFrame, 
    numOutputPartitions: int, 
    partitionCols: list, 
    tableName: str, 
    mode="overwrite"
):

    dfRepartitioned = (
        data.repartition(numOutputPartitions, partitionCols)
        .write.format("orc")
        .mode(f"{mode}")
        .saveAsTable(tableName)
    )