package uk.co.odinconsultants

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.{SparkConf, SparkContext}
import uk.co.odinconsultants.documentation_utils.Datum

import java.lang.reflect.Field
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import scala.util.{Failure, Success, Try}

object SparkUtils {
  val tmpDir: String = Files.createTempDirectory("SparkForTesting").toString

  val TABLE_NAME = "mysparktableagain"

  val highLevelObjectName = "default"

  val bucketName = "phbucketthatshouldreallynotexistyet"

  val REGION = "eu-west-2"

  val CATALOG = "iceberg"

  val DATABASE = "ph_tempdb"

  def getSession(app: String = "bdd_tests"): SparkSession = {
    val warehouseDir: String = s"s3://$bucketName/default"
    println(s"PH: warehouseDir = $warehouseDir")
    val master: String       = "local[2]"
    println(s"Using temp directory $tmpDir")
    System.setProperty("derby.system.home", tmpDir)
    val sparkConf: SparkConf =
      new SparkConf()
        .setMaster(master)
        .setAppName(app)
        .set(CATALOG_IMPLEMENTATION.key, "hive")
        .set(
          "spark.hadoop.hive.metastore.client.factory.class",
          "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
        )
        .set("spark.hadoop.hive.metastore.warehouse.dir", warehouseDir)
        .set(
          "spark.hadoop.fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider, org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider, com.amazonaws.auth.EnvironmentVariableCredentialsProvider, org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider",
        )
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.warehouse.dir", s"s3://$bucketName/default")
        .set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.hive.metastore.sharedPrefixes", "com.amazonaws.services.dynamodbv2")
//        .set("spark.sql.parquet.output.committer.class","com.amazonaws.emr.spark.EmrSparkSessionExtensions")
        .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .set("spark.sql.thriftserver.scheduler.pool", "fair")
        .set("spark.sql.ui.explainMode", "extended")
        .set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true")
        .set(
          "spark.sql.extensions",
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .set(s"spark.sql.catalog.$CATALOG", "org.apache.iceberg.spark.SparkCatalog")
        .set(s"spark.sql.catalog.$CATALOG.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .set(s"spark.sql.catalog.$CATALOG.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .set(s"spark.sql.catalog.$CATALOG.warehouse", s"s3://$bucketName/iceberg")
        .set(
          "spark.sql.extensions",
          "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .set("spark.sql.defaultCatalog", CATALOG) // this "activates" the settings above

    SparkContext.getOrCreate(sparkConf)
    SparkSession
      .builder()
      .appName(app)
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
  }

  /** Need to add
    * /home/henryp/Code/Java/iceberg/aws-bundle/build/libs/iceberg-aws-bundle-1.4.0-SNAPSHOT.jar
    * to the classpath
    */
  def main(args: Array[String]): Unit = Try {
    println(s"PH: bucketName = " + bucketName)
    val spark = getSession()

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
