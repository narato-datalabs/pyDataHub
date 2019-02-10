from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, first
from .entities import Hub

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyDataVault") \
    .getOrCreate()


class ProcessItem:

    def __init__(self, name, transformFunction, processFunction, entity):
        self.name = name
        self.transformFunction = transformFunction
        self.processFunction = processFunction
        self.entity = entity


class ProcessorBase:

    def __init__(self, dataHub):
        self.dataHub = dataHub
        self.processItems = []
        self.buildProcessItems()

    def buildProcessItems(self):
        pass

    def _getStagingData(self, batchName):
        return spark.createDataFrame([], StructType([]))

    def process(self, batchName):
        stagingDF = self._getStagingData(batchName)

        for item in self.processItems:
            dataToProcessDF = item.transformFunction(stagingDF, item.entity)
            item.processFunction(dataToProcessDF, item.entity)

    def buildContentToBeHashed(self):
        return ""

    def processHub(self, dataToBeProcessed, entity):
        joinExpression = entity.dataFrame["HASHKEY"] == dataToBeProcessed["HASHKEY"]

        newHubEntries = dataToBeProcessed.join(
            entity.dataFrame, joinExpression, "left_anti")
        newHubEntries.write.save(
            entity.storagePath, format="parquet", mode="append")
        entity.refreshData()

    def processSatellite(self, dataToBeProcessed, entity):
        ws = Window().partitionBy(entity.dataFrame["HASHKEY"]).orderBy(
            entity.dataFrame["LOADDATE"].desc())
        existingMostRecentHashesByHashKeyDF = entity.dataFrame.select(
            col("HASHKEY"), first("HASHDIFF").over(ws).alias("HASHDIFF")).distinct()

        joinExpression = (existingMostRecentHashesByHashKeyDF["HASHKEY"] == dataToBeProcessed[
            "HASHKEY"]) & (existingMostRecentHashesByHashKeyDF["HASHDIFF"] == dataToBeProcessed["HASHDIFF"])

        newHubEntries = dataToBeProcessed.join(
            existingMostRecentHashesByHashKeyDF, joinExpression, "left_anti")
        newHubEntries.write.save(
            entity.storagePath, format="parquet", mode="append")
        entity.refreshData()
