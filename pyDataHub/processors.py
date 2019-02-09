from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from .entities import Hub

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyDataVault") \
    .getOrCreate()


class ProcessItem:

    def _init(self, name, transformFunction, processFunction, entity):
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
            dataToProcessDF = item.transformFunction(stagingDF)
            item.processFunction(dataToProcessDF, item.entity)

    def buildContentToBeHashed(self):
        return ""

    def processHub(self, dataToBeProcessed, entity):
        joinExpression = entity.dataFrame["HASHKEY"] == dataToBeProcessed["HASHKEY"]

        newHubEntries = dataToBeProcessed.join(
            entity.dataFrame, joinExpression, "left_anti")
        newHubEntries.write.save(
            entity.storagePath, format="parquet", mode="append")

    def processSatellite(self, dataToBeProcessed, entity):
        joinExpression = entity.dataFrame["HASHKEY"] == dataToBeProcessed["HASHKEY"]

        # to do: process satellite contents
