from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyDataVault") \
    .getOrCreate()


class DataVault:

    def __init__(self, path, dbutils):
        self.path = path
        self.dbutils = dbutils

        self.initialize()
        self.registerHubs()

    def initialize(self):

        self.hubs = {}
        self.satellites = {}
        self.links = {}

        self.hubsPath = self.path + "/hubs"
        self.satellitesPath = self.path + "/satellites"
        self.linksPath = self.path + "/links"

        try:
            try:
                self.dbutils.fs.ls(self.path)
            except:
                self.dbutils.fs.mkdirs(self.path)
        except:
            raise OSError(
                100, "Could not create datavault root directory", self.path)

        try:
            self.dbutils.fs.ls(self.hubsPath)
        except:
            self.dbutils.fs.mkdirs(self.hubsPath)
        try:
            self.dbutils.fs.ls(self.satellitesPath)
        except:
            self.dbutils.fs.mkdirs(self.satellitesPath)
        try:
            self.dbutils.fs.ls(self.linksPath)
        except:
            self.dbutils.fs.mkdirs(self.linksPath)

    def registerHubs(self):
        pass
