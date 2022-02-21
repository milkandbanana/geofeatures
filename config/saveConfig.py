import yaml

sparkConf = {"spark.driver.memory": "16g",
             "spark.sql.shuffle.partitions": "1000",
             "spark.executor.cores": "8",
             "spark.executor.memory": "16g",
             "spark.dynamicAllocation.minExecutors": "32",
             "spark.dynamicAllocation.minExecutors": "160"}

tableConf = {"gridCentroids":"db.grid_with_centroids",
             "gridBuffers": "db.grid_with_buffers",
             "gkh":"db.reformagkh_",
             "osmGeometryTableName": "db.osm_geometry",
             "osmTagTableName": "db.osm_tag",
             "osmLoadDate": "2021-03-01"}

config = {"sparkConf": sparkConf, 
          "tableConf": tableConf}

with open("config/config.yaml", "w") as outfile:
    yaml.dump(config, outfile)