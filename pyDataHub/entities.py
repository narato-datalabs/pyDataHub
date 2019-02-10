from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyDataVault") \
    .getOrCreate()


class EntityTypeBase:

    def __init__(self, name, dataVault):
        self.name = name
        self.dataVault = dataVault
        self.dataFrame = None
        self.storagePath = self.dataVault.path
        self.satellites = {}

    def initialize(self):
        self.ensureStorageExists()
        self.ensureSchema()

    def entityTypeExists(self):
        try:
            self.dataVault.dbutils.fs.ls(self.storagePath)
            return True
        except:
            return False

    def ensureStorageExists(self):
        if self.entityTypeExists() == False:
            self.dataVault.dbutils.fs.mkdirs(self.storagePath)
            self.dataFrame = self.buildDataFrame()
            self.dataFrame.write.save(
                self.storagePath, format="parquet", mode='overwrite')
        else:
            self.refreshData()

    def refreshData(self):
        self.dataFrame = spark.read.parquet(self.storagePath)

    def buildDataFrame(self):
        self._fieldList = self.getSystemFieldsSchema() + self.getBusinessFieldsSchema()
        self._schema = StructType(self._fieldList)
        return spark.createDataFrame([], self._schema)

    def ensureSchema(self):
        pass

    def getSystemFieldsSchema(self):
        return [
            StructField("HASHKEY", StringType(), False),
            StructField("LOADDATE", StringType(), False)
        ]

    def getBusinessFieldsSchema(self):
        return []


class Hub(EntityTypeBase):

    def __init__(self, name, dataVault):
        super().__init__(name, dataVault)
        self.storagePath = self.dataVault.hubsPath + "/" + self.name
        self.initialize()
    #    trackingSatellite = RecordTrackingSatellite(self, dataVault)
    #    self.satellites[trackingSatellite.name] = trackingSatellite


class Satellite(EntityTypeBase):

    def __init__(self, name, hub, dataVault):
        super().__init__(name, dataVault)
        self.storagePath = self.dataVault.satellitesPath + "/" + self.name
        self.hub = hub
        self.initialize()

    def getSystemFieldsSchema(self):
        return super().getSystemFieldsSchema() + [
            StructField("RECORDSOURCE", StringType(), False),
            StructField("HASHDIFF", StringType(), False),
        ]


class RecordTrackingSatellite(Satellite):

    def __init__(self, hub, dataVault):
        super().__init__(hub.name + "_TS", hub, dataVault)

    def getBusinessFieldsSchema(self):
        return [
            StructField("SOURCESYSTEM", StringType(), False),
        ]
