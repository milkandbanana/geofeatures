import numpy as np
import pandas as pd
import yaml

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType

from itertools import product

class GridDistribution:
    def __init__(
        self,
        grid: DataFrame,
        gridID: str,
        gridGeometry: str,
        featuresData: DataFrame,
        featuresGeometry: str,
        numericalFeatures: list,
        categoricalFeatures: list,
        categoricalFunctions=["cnt"],
        numericalFunctions=["min", "max", "sum", "mean", "median", "std"],
    ):
        self.grid = grid
        self.gridID = gridID
        self.gridGeometry = gridGeometry
        self.featuresData = featuresData
        self.featuresGeometry = featuresGeometry

        # get built-in aggregate function for features
        if (categoricalFeatures is not None) & (categoricalFunctions == ["cnt"]):
            self.categoricalFeatures = categoricalFeatures
            self.categoricalFunctions = categoricalFunctions
            self.categoricalStat = [
                countDistinct(x).alias(f"cnt_{x}") for x in categoricalFeatures
            ]
        else:
            self.categoricalFeatures = []
            self.categoricalFunctions = []
            self.categoricalStat = []

        if (numericalFeatures is not None) & (numericalFunctions is not None):
            self.numericalFeatures = numericalFeatures
            self.numericalFunctions = numericalFunctions
            self.numericalStat = []
            if "sum" in numericalFunctions:
                self.numericalStat.extend(
                    [sum(x).alias(f"sum_{x}") for x in numericalFeatures]
                )
            if "mean" in numericalFunctions:
                self.numericalStat.extend(
                    [mean(x).alias(f"mean_{x}") for x in numericalFeatures]
                )
            if "median" in numericalFunctions:
                self.numericalStat.extend(
                    [
                        expr(f"percentile_approx({x}, 0.5)").alias(f"median_{x}")
                        for x in numericalFeatures
                    ]
                )
            if "max" in numericalFunctions:
                self.numericalStat.extend(
                    [max(x).alias(f"max_{x}") for x in numericalFeatures]
                )
            if "min" in numericalFunctions:
                self.numericalStat.extend(
                    [min(x).alias(f"min_{x}") for x in numericalFeatures]
                )
            if "std" in numericalFunctions:
                self.numericalStat.extend(
                    [stddev(x).alias(f"std_{x}") for x in numericalFeatures]
                )

        else:
            self.numericalFeatures = []
            self.numericalFunctions = []
            self.numericalStat = []

        self.features = self.numericalFeatures + self.categoricalFeatures
        self.featuresStats = self.categoricalStat + self.numericalStat


    def buffersDictionaries(self, dataAndAliases: dict) -> DataFrame:
        gridAlias = "gridTbl"
        grid = self.grid
    
        for bufferGeometry in dataAndAliases.keys():
    
            bufferAlias = dataAndAliases[bufferGeometry]
            bufferColumnsAlias = [f"{bufferAlias}.{self.gridID} as {self.gridID}"]
    
            gridColumnsAlias = [f"{gridAlias}.{self.gridID} as {bufferAlias}"]
    
            newColumnsAlias = bufferColumnsAlias + gridColumnsAlias
    
            grid = (
                grid.alias(f"{gridAlias}")
                .join(
                    grid.alias(f"{bufferAlias}"),
                    expr(
                        f"ST_Intersects(ST_GeomFromWKT({bufferAlias}.{bufferGeometry}), ST_GeomFromWKT({gridAlias}.{self.gridGeometry}))"
                    ),
                )
                .selectExpr(newColumnsAlias)
            )
    
        outputData = grid
    
        return outputData


    def featuresByGrid(self) -> DataFrame:
        featuresAlias = "featuresTbl"
        gridAlias = "gridTbl"

        initFeaturesColumns = [f"{self.featuresGeometry}"]
        columns = initFeaturesColumns + self.features

        initColumnsAlias = [f"{gridAlias}.{self.gridID} as {self.gridID}"]

        joinFeaturesAlias = list(
            map(
                lambda feature: f"{featuresAlias}.{feature} as {feature}", self.features
            )
        )
        joinColumnsAlias = initColumnsAlias + joinFeaturesAlias

        intersectsGridAndFeatures = (
            self.grid.alias(f"{gridAlias}")
            .join(
                self.featuresData.alias(f"{featuresAlias}")
                .select(columns)
                .filter(col(f"{self.featuresGeometry}").isNotNull())
                .filter(expr(f"ST_IsValid(ST_GeomFromWKT({self.featuresGeometry}))"))
                # in case of error for invalid geometry
                # .withColumn(
                #    f"{featuresGeometry}",
                #    expr(f"ST_SimplifyPreserveTopology(ST_GeomFromWKT({featuresGeometry}), 0)"))
                .alias(f"{featuresAlias}"),
                expr(
                    f"ST_Intersects(ST_GeomFromWKT({gridAlias}.{self.gridGeometry}), ST_GeomFromWKT({featuresAlias}.{self.featuresGeometry}))"
                ),
            )
            .selectExpr(joinColumnsAlias)
        )

        outputData = intersectsGridAndFeatures.na.fill(0)

        return outputData


    def gridFeaturesAgg(
        self,
        featuresOnGrid,
        bufferDictionary: DataFrame,
        bufferColumnName: str,
    ) -> DataFrame:

        bufferDictAlias = "dictionary"
        featOnGridAlias = "grid"

        if (
            (bufferDictionary is None)
            | (bufferDictionary == "")
            | (bufferColumnName is None)
            | (bufferColumnName == "")
        ):
            outputData = featuresOnGrid.groupBy(self.gridID).agg(*self.featuresStats)

        else:
            outputData = (
                featuresOnGrid.alias(f"{featOnGridAlias}")
                .join(
                    bufferDictionary.alias(f"{bufferDictAlias}"),
                    expr(
                        f"{bufferDictAlias}.{bufferColumnName} == {featOnGridAlias}.{self.gridID}"
                    ),
                    "inner",
                )
                .groupBy(col(f"{bufferDictAlias}.{self.gridID}"))
                .agg(*self.featuresStats)
            )
        return outputData.na.fill(0)


    def gridDistances(
        self,
        featurePopulation: str,
        binPopulation: str,
        gridCentroidGeometryLon: str,
        gridCentroidGeometryLat: str,
        featuresCentroidGeometryLon: str,
        featuresCentroidGeometryLat: str,
    ) -> DataFrame:
        
        # distance in km
        @pandas_udf(FloatType())
        def calcDistKm(
            in_lon1: pd.Series,
            in_lat1: pd.Series,
            in_lon2: pd.Series,
            in_lat2: pd.Series,
        ):
            # convert decimal degrees to radians
            lon1 = np.radians(in_lon1)
            lat1 = np.radians(in_lat1)
            lon2 = np.radians(in_lon2)
            lat2 = np.radians(in_lat2)

            # haversine formula
            dlon = lon2 - lon1
            dlat = lat2 - lat1

            a = np.power(np.sin(dlat / 2.0), 2) + np.cos(lat1) * np.cos(
                lat2
            ) * np.power(np.sin(dlon / 2.0), 2)
            c = 2.0 * np.arcsin(np.sqrt(a))
            earthRadiusKm = 6371  # Radius of earth in kilometers
            distanceKm = np.round(c * earthRadiusKm, 2)

            return distanceKm

        gridAlias = "gridTbl"
        featuresAlias = "featuresTbl"
        
        # distance column name
        distColumnName = "distance"
        
        # range for population bin
        populations = tuple(
            [
                (0, 100),
                (100, 500),
                (500, 1000),
                (1000, 5000),
                (5000, 10000),
                (10000, 50000),
                (50000, 100000),
                (100000, 500000),
                (500000, 1000000),
                (1000000, 2000000),
                (2000000, 5000000),
                (5000000, 1000000000),
            ]
        )
        
        # group by this column
        initGridColumns = [f"{self.gridID}", binPopulation]

        binPopulationCnd = (
            "CASE "
            + " ".join(
                list(
                    map(
                        lambda population: f""" WHEN {featurePopulation} >= {population[0]} AND \
                                            {featurePopulation} < {population[1]} THEN '{population[1]}' """,
                        populations,
                    )
                )
            )
            + "ELSE 0 END"
        )

        featuresData = self.featuresData.withColumn(
            binPopulation, expr(binPopulationCnd)
        )

        CachedGrid = self.grid.cache()
        
        # stat for distance
        distStat = [min(col(distColumnName)).alias(f"min_{distColumnName}")]

        outputData = (
            CachedGrid.alias(gridAlias)
            .crossJoin(featuresData.alias(featuresAlias))
            .withColumn(
                distColumnName,
                calcDistKm(
                    f"{gridAlias}.{gridCentroidGeometryLon}",
                    f"{gridAlias}.{gridCentroidGeometryLat}",
                    f"{featuresAlias}.{featuresCentroidGeometryLon}",
                    f"{featuresAlias}.{featuresCentroidGeometryLat}",
                ),
            )
            .groupBy(initGridColumns)
            .agg(*distStat)
        )

        CachedGrid.unpersist()
        return outputData
    
    
    def joining(self, dataAndAliases: dict) -> DataFrame:
        outputData = self.grid
        gridAlias = "gridTbl"
        previousFeatures = []
        initColumnsAlias = [f"{gridAlias}.{self.gridID} as {self.gridID}"]

        # TO DO: joinColumns And features must be lists of str

        # get actual names of categorical features
        categoricalNames = list(
            map(
                lambda name: f"{name[0]}_{name[1]}",
                product(self.categoricalFunctions, self.categoricalFeatures),
            )
        )

        # get actual names of numerical features
        numericalNames = list(
            map(
                lambda name: f"{name[0]}_{name[1]}",
                product(self.numericalFunctions, self.numericalFeatures),
            )
        )
        featuresNames = categoricalNames + numericalNames

        # left join with grid and fill nulls with zeros to ensure that each grid cell has defined feature value
        for featuresDataFrame in dataAndAliases.keys():

            featuresAlias = dataAndAliases[featuresDataFrame]

            joinFeaturesAlias = list(
                map(
                    lambda feature: f"{featuresAlias}.{feature} as {feature}_{featuresAlias}",
                    featuresNames,
                )
            )
            joinColumnsAlias = joinFeaturesAlias

            outputData = (
                outputData.alias(f"{gridAlias}")
                .join(featuresDataFrame.alias(f"{featuresAlias}"), f"{self.gridID}", "left")
                .selectExpr(initColumnsAlias + previousFeatures + joinColumnsAlias)
            )

            previousFeatures += list(
                map(
                    lambda feature: f"{gridAlias}.{feature}_{featuresAlias} as {feature}_{featuresAlias}",
                    featuresNames,
                )
            )

        outputData = outputData.na.fill(0)
        return outputData
    
    
    def featuresByGridNaive(self) -> DataFrame:

        featuresAlias = "featuresTbl"
        gridAlias = "gridTbl"

        # UDF aggregate function as example
        # @pandas_udf(FloatType(), PandasUDFType.GROUPED_AGG)
        # def medianUDF(x: pd.Series):
        #    return x.median()
        # featuresStatsUdf = [
        #     medianUDF(x).alias(f"median_{x}") for x in self.numericalFeatures
        # ]

        initFeaturesColumns = [f"{self.featuresGeometry}"]
        columns = initFeaturesColumns + self.features

        initColumnsAlias = [f"{gridAlias}.{self.gridID} as {self.gridID}"]

        joinFeaturesAlias = list(
            map(
                lambda feature: f"{featuresAlias}.{feature} as {feature}", self.features
            )
        )
        joinColumnsAlias = initColumnsAlias + joinFeaturesAlias

        intersectsGridAndFeatures = (
            self.grid.alias(f"{gridAlias}")
            .join(
                self.featuresData.alias(f"{featuresAlias}")
                .select(columns)
                .filter(col(f"{self.featuresGeometry}").isNotNull())
                .filter(expr(f"ST_IsValid(ST_GeomFromWKT({self.featuresGeometry}))"))
                # in case of error for invalid geometry
                # .withColumn(
                #    f"{featuresGeometry}",
                #    expr(f"ST_SimplifyPreserveTopology(ST_GeomFromWKT({featuresGeometry}), 0)"))
                .alias(f"{featuresAlias}"),
                expr(
                    f"ST_Intersects(ST_GeomFromWKT({gridAlias}.{self.gridGeometry}), ST_GeomFromWKT({featuresAlias}.{self.featuresGeometry}))"
                ),
            )
            .selectExpr(joinColumnsAlias)
        )

        # built-in and UDF aggregate functions and join them on gridID
        # (cause we cannot mixture them together)
        # outputData = (
        #    intersectsGridAndFeatures.groupBy(self.gridID)
        #    .agg(*featuresStats)
        #    .join(
        #        intersectsGridAndFeatures.groupBy(self.gridID).agg(*featuresStatsUdf),
        #        self.gridID,
        #        "inner",
        #    )
        # )

        outputData = intersectsGridAndFeatures.groupBy(self.gridID).agg(
            *self.featuresStats
        )
        return outputData
