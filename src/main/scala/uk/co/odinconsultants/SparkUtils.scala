package uk.co.odinconsultants

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.{SparkConf, SparkContext}
import uk.co.odinconsultants.documentation_utils.Datum

import java.lang.reflect.Field
import java.nio.file.Files
import scala.util.{Failure, Success, Try}

object SparkUtils {
  val tmpDir: String = Files.createTempDirectory("SparkForTesting").toString

  val TABLE_NAME = "mysparktableagain"

  val highLevelObjectName = "default"

  val bucketName = "phbucketthatshouldreallynotexistyet"

  val CATALOG = "iceberg"

  val DATABASE = "ph_tempdb"

  val warehouseDir: String = s"s3://$bucketName/default"

  val properties: Map[String, String] = Map(
    CATALOG_IMPLEMENTATION.key                                      -> "hive",
    "spark.hadoop.hive.metastore.client.factory.class"              ->
      "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
    "spark.hadoop.hive.metastore.warehouse.dir"                     -> warehouseDir,
    "spark.hadoop.fs.s3a.aws.credentials.provider"                  ->
      "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider,org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider,com.amazonaws.auth.EnvironmentVariableCredentialsProvider,org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
    "spark.hadoop.fs.s3a.impl"                                      -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.warehouse.dir"                                       -> s"s3://$bucketName/default",
    "spark.hadoop.fs.s3.impl"                                       -> "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.sql.hive.metastore.sharedPrefixes"                       -> "com.amazonaws.services.dynamodbv2",
    //        "spark.sql.parquet.output.committer.class" ->"com.amazonaws.emr.spark.EmrSparkSessionExtensions",
    "spark.sql.sources.partitionOverwriteMode"                      -> "dynamic",
    "spark.sql.thriftserver.scheduler.pool"                         -> "fair",
    "spark.sql.ui.explainMode"                                      -> "extended",
    "spark.sql.parquet.fs.optimized.committer.optimization-enabled" -> "true",
    "spark.sql.extensions"                                          ->
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    s"spark.sql.catalog.$CATALOG"                                   -> "org.apache.iceberg.spark.SparkCatalog",
    s"spark.sql.catalog.$CATALOG.catalog-impl"                      -> "org.apache.iceberg.aws.glue.GlueCatalog",
    s"spark.sql.catalog.$CATALOG.io-impl"                           -> "org.apache.iceberg.aws.s3.S3FileIO",
    s"spark.sql.catalog.$CATALOG.warehouse"                         -> s"s3://$bucketName/iceberg",
    "spark.sql.extensions"                                          ->
      "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.defaultCatalog"                                      -> CATALOG,
  )

  val getSession: SparkSession = {
    println(s"PH: warehouseDir = $warehouseDir")
    val master: String       = "local[2]"
    println(s"Using temp directory $tmpDir")
    System.setProperty("derby.system.home", tmpDir)
    val sparkConf: SparkConf =
      new SparkConf()
        .setMaster(master)
        .setAppName("bdd_tests")

    properties.foreach { case(k: String, v: String) =>
      sparkConf.set(k, v)
    }

    SparkContext.getOrCreate(sparkConf)
    SparkSession
      .builder()
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
  }

  def main(args: Array[String]): Unit = Try {
    println(s"PH: bucketName = " + bucketName)
    val spark = getSession

    val s3Utils = new S3Utils(bucketName)

    val table          = s"$CATALOG.$DATABASE.$TABLE_NAME"
    val exits: Boolean = spark.catalog.tableExists(table)
    println(s"Table $table Exits $exits")
    if (exits) spark.sql(s"DROP TABLE $table")

    s3Utils.deleteObjectsMatching(
      s3Utils.getObjectNamesIn(bucketName),
      bucketName,
      highLevelObjectName,
    )
    s3Utils.deleteObjectsMatching(s3Utils.getObjectNamesIn(bucketName), bucketName, "iceberg")

    val sql: String = createDatumTable(table)
    println(sql)
    spark.sql(sql)

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

  def createDatumTable(tableName: String): String = {
    val fields: String = classOf[Datum].getDeclaredFields
      .map { field: Field =>
        s"${field.getName} ${field.getType.getSimpleName}"
      }
      .mkString(",\n")
    // Database my_catalog not found. (Service: Glue, Status Code: 400, Request ID: 3820aa4a-f29f-433e-9c07-7968d0747207)
    s"""CREATE TABLE $tableName ($fields)""".stripMargin
  }
}
