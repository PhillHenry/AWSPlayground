package uk.co.odinconsultants

import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, WAREHOUSE_PATH}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.lang
import java.nio.file.Files

object SparkUtils {
  val tmpDir: String = Files.createTempDirectory("SparkForTesting").toString

  val sparkSession: SparkSession = getSession("bdd_tests")

  def getSession(app: String = "bdd_tests"): SparkSession = {
    val master   : String    = "local[2]"
    val sparkConf: SparkConf = {
      println(s"Using temp directory $tmpDir")
      System.setProperty("derby.system.home", tmpDir)
      new SparkConf()
        .setMaster(master)
        .setAppName(app)
        .set("hive.metastore client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactor")
        .set(CATALOG_IMPLEMENTATION.key, "hive")
        .set("spark.sql.catalog.local.type", "hadoop")
        .set("aws.glue.cache.table.enable", "true")
        .set("aws.glue.cache.table.size", "1000")
        .set("aws.glue.cache.table.ttl-mins", "30")
//        .set(DEFAULT_CATALOG.key, "local")
        .set(WAREHOUSE_PATH.key, tmpDir)
        .set("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("spark.hadoop.hive.metastore.warehouse.dir","s3://phbucketthatshouldreallynotexistyet/default")
        .set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider, org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider, com.amazonaws.auth.EnvironmentVariableCredentialsProvider, org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
        .set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.warehouse.dir","hdfs:///user/spark/warehouse")
        .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalogImplementation","hive")
        .set("spark.sql.catalog.iceberg","org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.iceberg.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
        .set("spark.sql.catalog.iceberg.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
        .set("#spark.sql.catalog.iceberg.lock-impl","org.apache.iceberg.aws.dynamodb.DynamoDbLockManager")
        .set("#spark.sql.catalog.iceberg.lock.table","IcebergLockTable")
        .set("spark.sql.catalog.iceberg.warehouse","s3://phbucketthatshouldreallynotexistyet/iceberg")
        .set("spark.sql.emr.internal.extensions","com.amazonaws.emr.spark.EmrSparkSessionExtensions")
        .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.hive.metastore.sharedPrefixes","com.amazonaws.services.dynamodbv2")
        .set("spark.sql.parquet.output.committer.class","com.amazon.emr.committer.EmrOptimizedSparkSqlParquetOutputCommitter")
        .set("spark.sql.sources.partitionOverwriteMode","dynamic")
        .set("spark.sql.thriftserver.scheduler.pool","fair")
        .set("spark.sql.ui.explainMode","extended")
        .set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true")


        .set("aws.glue.endpoint","https://glue.eu-west-2.amazonaws.com")
        .set("aws.glue.region","eu-west-2")
        .set("aws.glue.connection-timeout","30000")
        .set("aws.glue.socket-timeout","30000")
        .set("hive.imetastoreclient.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("hive.metastore.uris","thrift://FOR_YOUR_METASTORE_URI_SEE_SPARK_UI_ENVIRONMENT_TAB:9083")
        .set("hive.metastore.warehouse.dir","s3://phbucketthatshouldreallynotexistyet/default")
        .set("hive.metastore.connect.retries","15")
        .set("aws.glue.cache.table.enable","true")
        .set("aws.glue.cache.table.size","1000")
        .set("aws.glue.cache.table.ttl-mins","30")
        .set("aws.glue.cache.db.enable","true")
        .set("aws.glue.cache.db.size","1000")
        .set("aws.glue.cache.db.ttl-mins","30")


        .setSparkHome(tmpDir)
    }
    SparkContext.getOrCreate(sparkConf)
    SparkSession
      .builder()
      .appName(app)
      .master("local[2]")
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = {
    val accessKey = args(0)
    val secretKey = args(1)
    val spark = getSession()
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", accessKey)
    spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", secretKey)
    val result_df = spark.range(1000)
    result_df.write//.partitionBy("my_column")
    .option("fs.s3a.committer.name", "partitioned")
      .option("fs.s3a.committer.staging.conflict-mode", "replace")
      .option("fs.s3a.fast.upload.buffer", "bytebuffer")
      .mode("overwrite")
      .csv(path="s3a://phbucketthatshouldreallynotexistyet/output")
  }
}
