package uk.co.odinconsultants

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.internal.StaticSQLConf.{CATALOG_IMPLEMENTATION, WAREHOUSE_PATH}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import uk.co.odinconsultants.documentation_utils.Datum

import java.lang
import java.lang.reflect.Field
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import scala.util.{Failure, Success, Try}

object SparkUtils {
  val tmpDir: String = Files.createTempDirectory("SparkForTesting").toString

  val TABLE_NAME = "mysparktable"

  val highLevelObjectName = "default"

  val bucketName = "phbucketthatshouldreallynotexistyet"

  def getSession(app: String = "bdd_tests"): SparkSession = {
    val warehouseDir: String = s"s3://$bucketName/default"
    println(s"PH: warehouseDir = $warehouseDir")
    val master   : String    = "local[2]"
    println(s"Using temp directory $tmpDir")
    System.setProperty("derby.system.home", tmpDir)
    val sparkConf: SparkConf = {
      new SparkConf()
        .setMaster(master)
        .setAppName(app)

        .set(CATALOG_IMPLEMENTATION.key, "hive")

        .set("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("spark.hadoop.hive.metastore.warehouse.dir",warehouseDir)
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

        .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.iceberg","org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.iceberg.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
        .set("spark.sql.catalog.iceberg.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
        .set("spark.sql.catalog.iceberg.warehouse","s3://phbucketthatshouldreallynotexistyet/iceberg")
        .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
//        .set("spark.sql.defaultCatalog", "iceberg") // this "activates" the settings above


        .set("aws.glue.endpoint","https://glue.eu-west-2.amazonaws.com")
        .set("aws.glue.region","eu-west-2")
        .set("aws.glue.connection-timeout","30000")
        .set("aws.glue.socket-timeout","30000")
        .set("hive.imetastoreclient.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("spark.sql.catalog.hive_prod.uri", "thrift://hivemetastore-hive-metastore:9083")
        .set("hive.metastore.warehouse.dir","s3://phbucketthatshouldreallynotexistyet/default")
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
      .master("local[2]").enableHiveSupport()
      .getOrCreate()
  }

  def configureHadoop(conf: Configuration): Unit = {
    val hiveWarehouseDir: String = "s3://" + bucketName + "/" + highLevelObjectName

    println(s"PH: $hiveWarehouseDir")

    conf.set("aws.glue.endpoint","https://glue.eu-west-2.amazonaws.com")
    conf.set("aws.glue.region","eu-west-2")
    conf.set("aws.glue.connection-timeout","30000")
    conf.set("aws.glue.socket-timeout","30000")
    conf.set("hive.imetastoreclient.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    conf.set("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    //    conf.set("hive.metastore.uris","thrift://hivemetastore-hive-metastore:9083")
    //    conf.set("spark.sql.catalog.hive_prod.uri", "thrift://hivemetastore-hive-metastore:9083")
    //    conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.WebIdentityTokenCredentialsProvider")
    conf.set("hive.metastore.warehouse.dir",hiveWarehouseDir)
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

    conf.set("spark.sql.defaultCatalog", "my_catalog")
    conf.set("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.my_catalog.warehouse", s"s3://$bucketName/my/key/prefix")
    conf.set("spark.sql.catalog.my_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set("spark.sql.catalog.my_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
  }

  def main(args: Array[String]): Unit = Try {
    println(s"PH: bucketName = " + bucketName)
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

    createDatumTable(TABLE_NAME)
    spark.createDataFrame(createData(5, new java.sql.Date(new java.util.Date().getTime), 2, 10000, 30))
    result_df.writeTo(TABLE_NAME).create()
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

  def createData(num_partitions: Int, now: Date, dayDelta: Int, tsDelta: Long, num_rows: Int): Seq[Datum] = {
    val DayMS: Long = 24 * 60 * 60 * 1000

    val today = new Date((now.getTime / DayMS).toLong * DayMS)
    Seq.range(0, num_rows).map((i: Int) => Datum(
      i,
      s"label_$i",
      i % num_partitions,
      new Date(today.getTime + (i * DayMS * dayDelta)),
      new Timestamp(now.getTime + (i * tsDelta)))
    )
  }
  def createDatumTable(tableName: String): String = {
    val fields: String = classOf[Datum].getDeclaredFields
      .map { field: Field =>
        s"${field.getName} ${field.getType.getSimpleName}"
      }
      .mkString(",\n")
    s"""CREATE TABLE $tableName ($fields)""".stripMargin
  }
}
