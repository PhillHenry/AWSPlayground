package uk.co.odinconsultants

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, WAREHOUSE_PATH}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.lang
import java.nio.file.Files
import scala.util.{Failure, Success, Try}

object SparkUtils {
  val tmpDir: String = Files.createTempDirectory("SparkForTesting").toString

  val sparkSession: SparkSession = getSession("bdd_tests")

  val highLevelObjectName = "default"

  private val bucketName = "phbucketthatshouldreallynotexistyet"

  def getSession(app: String = "bdd_tests"): SparkSession = {
    val master   : String    = "local[2]"
    val sparkConf: SparkConf = {
      println(s"Using temp directory $tmpDir")
      System.setProperty("derby.system.home", tmpDir)
      new SparkConf()
        .setMaster(master)
        .setAppName(app)

        .set(CATALOG_IMPLEMENTATION.key, "hive")

        .set("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("spark.hadoop.hive.metastore.warehouse.dir","s3://" + bucketName + "/default")
        .set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider, org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider, com.amazonaws.auth.EnvironmentVariableCredentialsProvider, org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
        .set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.warehouse.dir","s3://phbucketthatshouldreallynotexistyet/default")
        .set("spark.hadoop.fs.s3.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.hive.metastore.sharedPrefixes","com.amazonaws.services.dynamodbv2")
//        .set("spark.sql.parquet.output.committer.class","com.amazonaws.emr.spark.EmrSparkSessionExtensions")
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
        .set("spark.sql.catalog.hive_prod.uri", "thrift://hivemetastore-hive-metastore:9083")
        .set("hive.metastore.warehouse.dir","s3://" + bucketName + "/default")
        .set("hive.metastore.connect.retries","15")
        .set("aws.glue.cache.table.enable","true")
        .set("aws.glue.cache.table.size","1000")
        .set("aws.glue.cache.table.ttl-mins","30")
        .set("aws.glue.cache.db.enable","true")
        .set("aws.glue.cache.db.size","1000")
        .set("aws.glue.cache.db.ttl-mins","30")
//
//        .set("hive.metastore.uris", "thrift://hivemetastore-hive-metastore:9083")
//        .set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.WebIdentityTokenCredentialsProvider")
//
//        .set("spark.sql.catalog.spark_catalog.type","hive")
//        .set("spark.sql.catalog.hive_prod.type","hive")
//        .set("spark.sql.catalog.hive_prod.uri", "thrift://hivemetastore-hive-metastore:9083")

//        .setSparkHome(tmpDir)
    }
    SparkContext.getOrCreate(sparkConf)
    SparkSession
      .builder()
      .appName(app)
      .master("local[2]")
      .getOrCreate()
  }

  def configureHadoop(conf: Configuration): Unit = {
    conf.set("aws.glue.endpoint","https://glue.eu-west-2.amazonaws.com")
    conf.set("aws.glue.region","eu-west-2")
    conf.set("aws.glue.connection-timeout","30000")
    conf.set("aws.glue.socket-timeout","30000")
    conf.set("hive.imetastoreclient.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    conf.set("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
//    conf.set("hive.metastore.uris","thrift://hivemetastore-hive-metastore:9083")
//    conf.set("spark.sql.catalog.hive_prod.uri", "thrift://hivemetastore-hive-metastore:9083")
//    conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.WebIdentityTokenCredentialsProvider")
    conf.set("hive.metastore.warehouse.dir","s3://" + bucketName + "/" + highLevelObjectName)
    conf.set("hive.metastore.connect.retries","15")
    conf.set("aws.glue.cache.table.enable","true")
    conf.set("aws.glue.cache.table.size","1000")
    conf.set("aws.glue.cache.table.ttl-mins","30")
    conf.set("aws.glue.cache.db.enable","true")
    conf.set("aws.glue.cache.db.size","1000")
    conf.set("aws.glue.cache.db.ttl-mins","30")
//
//
//    conf.set("hive.metastore.uris", "thrift://hivemetastore-hive-metastore:9083")
//    conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.WebIdentityTokenCredentialsProvider")
//
//    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
//    conf.set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
//
//    conf.set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
//    conf.set("spark.sql.catalog.spark_catalog.type","hive")
//    conf.set("spark.sql.catalog.hive_prod","org.apache.iceberg.spark.SparkCatalog")
//    conf.set("spark.sql.catalog.hive_prod.type","hive")
//    conf.set("spark.sql.catalog.hive_prod.uri", "thrift://hivemetastore-hive-metastore:9083")

  }

  def main(args: Array[String]): Unit = Try {
    val spark = getSession()
    configureHadoop(spark.sparkContext.hadoopConfiguration)
    val result_df = spark.range(1000)
//    result_df.write//.partitionBy("my_column")
//    .option("fs.s3a.committer.name", "partitioned")
//      .option("fs.s3a.committer.staging.conflict-mode", "replace")
//      .option("fs.s3a.fast.upload.buffer", "bytebuffer")
//      .mode("overwrite")
//      .csv(path="s3a://phbucketthatshouldreallynotexistyet/output")

    // you need to run recursively:
    // aws s3 rm s3://phbucketthatshouldreallynotexistyet/default/
    val s3Utils = new S3Utils(bucketName)
    val keys = s3Utils.getObjectNamesIn(bucketName)
    s3Utils.deleteObjectsMatching(keys, bucketName, highLevelObjectName)
    result_df.write.mode("append").saveAsTable("mysparktable")
    s3Utils.getObjectNamesIn(bucketName).forEach { obj: String =>
      println("Object = " + obj);
    }
  } match {
      case Success(x) =>
        println("Result: Success")
      case Failure(x) =>
        println("Result: Failed!")
        x.printStackTrace()
    }
}
