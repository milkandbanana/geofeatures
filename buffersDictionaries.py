import yaml

from geospark.register import GeoSparkRegistrator
from pyspark import SparkConf, SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from tqdm import tqdm

config = yaml.load(open(r"config/config.yaml"), Loader=yaml.FullLoader)
conf = SparkConf().setAll(config["sparkConf"].items())

spark = (
    SparkSession.builder.appName("BuffersDictionaries")
    .config(conf=conf)
    .enableHiveSupport()
    .getOrCreate()
)

GeoSparkRegistrator.registerAll(spark)

from lib.distribution import *
from lib.utils import *

gridTbl = config["tableConf"]["gridBuffers"]
gridID = "gid"
gridGeometry = "geom_wkt"
bufferGeometries = {
    "buffer_1000": "buffer_1km",
    "buffer_2000": "buffer_2km",
    "buffer_3000": "buffer_3km",
    "buffer_4000": "buffer_4km",
    "buffer_5000": "buffer_5km",
}

regionIDs = gridWithBuffers.select(
    collect_set(col("region_id")).alias("region_id")
).first()["region_id"]

dbName = ""

for buffer in list(bufferGeometries.items()):

    bufferKey = buffer[0]
    bufferVal = buffer[1]

    toWriteTbl = f"{dbName}.grid_dict_{bufferVal}"

    for regionNumber, region in enumerate(regionIDs):

        gridWithBuffers = spark.table(gridTbl).filter(col("region_id") == region)

        gridDistribs = GridDistribution(
            gridWithBuffers,
            gridID,
            gridGeometry,
            featuresData=None,
            featuresGeometry=None,
            numericalFeatures=None,
            categoricalFeatures=None,
        )

        localDict = GridDistribution.buffersDictionaries(
            {bufferKey: bufferVal},
        ).select(col(f"{gridID}"), col(f"{bufferVal}"))

        if regionNumber == 0:
            saveTableOverwritePartition(
                localDict, 50, [gridID], toWriteTbl, "overwrite"
            )

        else:
            saveTableOverwritePartition(localDict, 50, [gridID], toWriteTbl, "append")
