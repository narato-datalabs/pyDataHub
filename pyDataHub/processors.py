from pyspark.sql import SparkSession
from .entities import Hub

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyDataVault") \
    .getOrCreate()


class FactSingleSatelliteProcessorBase:

    def __init__(self, dataHub):
        self.dataHub = dataHub
        self.satellite = None

    def _getStagingData(self, batchName):
        return 1

    def _transformStagingData(self, stagingDataFrame):
        return 1

    def _getHubJoinExpression(self, hubDataFrame, toProcessDataFrame):
        return 1

    def process(self, batchName):
        stagingDF = self._getStagingData(batchName)
        dataToProcessDF = self._transformStagingData(stagingDF)
        hubJoinExpression = self._getHubJoinExpression(
            self.satellite.hub.dataFrame, dataToProcessDF)

        newHubEntries = dataToProcessDF.join(
            self.satellite.hub.dataFrame, hubJoinExpression, "left_anti")
        newHubEntries.write.save(
            self.satellite.hub.storagePath, format="parquet", mode="append")
