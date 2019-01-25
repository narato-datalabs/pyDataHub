from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("pyDataVault") \
    .getOrCreate()


class DataHub:

    def __init__(self, path, dbutils):
        self.path = path
        self.dbutils = dbutils
        self.factsProcessors = {}
        self.businessProcessors = {}
        self.factsDataVault = None
        self.businessDataVault = None

        self.stagingPath = self.path + "/staging"
        self.factsDataVaultPath = self.path + "/facts"

        try:
            try:
                self.dbutils.fs.ls(self.path)
            except:
                self.dbutils.fs.mkdirs(self.path)
        except:
            raise OSError(
                100, "Could not create datahub root directory", self.path)

        try:
            self.dbutils.fs.ls(self.stagingPath)
        except:
            self.dbutils.fs.mkdirs(self.stagingPath)
        try:
            self.dbutils.fs.ls(self.factsDataVaultPath)
        except:
            self.dbutils.fs.mkdirs(self.factsDataVaultPath)

    def registerFactsProcessors(self):
        pass
