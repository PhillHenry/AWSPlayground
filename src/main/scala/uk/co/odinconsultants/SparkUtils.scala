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
    val master   : String    = "local[2]"
    println(s"Using temp directory $tmpDir")
    System.setProperty("derby.system.home", tmpDir)
    val sparkConf: SparkConf = {
      new SparkConf()
        .setMaster(master)
        .setAppName(app)

        .set("spark.jars", "/home/henryp/Code/Java/iceberg/aws-bundle/build/libs/iceberg-aws-bundle-1.4.0-SNAPSHOT.jar")

        .set(CATALOG_IMPLEMENTATION.key, "hive")

        .set("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("spark.hadoop.hive.metastore.warehouse.dir",warehouseDir)
        .set("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider, org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider, com.amazonaws.auth.EnvironmentVariableCredentialsProvider, org.apache.hadoop.fs.s3a.auth.IAMInstanceCredentialsProvider")
        .set("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.warehouse.dir",s"s3://$bucketName/default")
        .set("spark.hadoop.fs.s3.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.hive.metastore.sharedPrefixes","com.amazonaws.services.dynamodbv2")
//        .set("spark.sql.parquet.output.committer.class","com.amazonaws.emr.spark.EmrSparkSessionExtensions")
        .set("spark.sql.sources.partitionOverwriteMode","dynamic")
        .set("spark.sql.thriftserver.scheduler.pool","fair")
        .set("spark.sql.ui.explainMode","extended")
        .set("spark.sql.parquet.fs.optimized.committer.optimization-enabled", "true")

        .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set(s"spark.sql.catalog.$CATALOG","org.apache.iceberg.spark.SparkCatalog")
        .set(s"spark.sql.catalog.$CATALOG.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog")
        .set(s"spark.sql.catalog.$CATALOG.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
        .set(s"spark.sql.catalog.$CATALOG.warehouse",s"s3://$bucketName/iceberg")
        .set("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.defaultCatalog", CATALOG) // this "activates" the settings above


        .set("aws.glue.endpoint","https://glue.eu-west-2.amazonaws.com")
        .set("aws.glue.region",REGION)
        .set("aws.glue.connection-timeout","30000")
        .set("aws.glue.socket-timeout","30000")
        .set("hive.imetastoreclient.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("hive.metastore.client.factory.class","com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
        .set("spark.sql.catalog.hive_prod.uri", "thrift://hivemetastore-hive-metastore:9083")
        .set("hive.metastore.warehouse.dir",s"s3://$bucketName/default")
        .set("hive.metastore.connect.retries","15")
        .set("aws.glue.cache.table.enable","true")
        .set("aws.glue.cache.table.size","1000")
        .set("aws.glue.cache.table.ttl-mins","30")
        .set("aws.glue.cache.db.enable","true")
        .set("aws.glue.cache.db.size","1000")
        .set("aws.glue.cache.db.ttl-mins","30")

        .setJars(List("file:///home/henryp/Code/Java/iceberg/aws-bundle/build/libs/iceberg-aws-bundle-1.4.0-SNAPSHOT.jar"))

    }
    SparkContext.getOrCreate(sparkConf)
    SparkSession
      .builder()
      .appName(app)
      .master("local[2]").enableHiveSupport()
      .getOrCreate()
  }

  /**
   * Need to add
   * /home/henryp/Code/Java/iceberg/aws-bundle/build/libs/iceberg-aws-bundle-1.4.0-SNAPSHOT.jar
   * to the classpath
   */
  def main(args: Array[String]): Unit = Try {
    println(s"PH: bucketName = " + bucketName)
    val spark = getSession()

    val s3Utils = new S3Utils(bucketName)

    val table = s"$CATALOG.$DATABASE.$TABLE_NAME"
    val exits: Boolean = spark.catalog.tableExists(table)
    println(s"Table $table Exits $exits")
    if (exits) spark.sql(s"DROP TABLE $table")

    s3Utils.deleteObjectsMatching(s3Utils.getObjectNamesIn(bucketName), bucketName, highLevelObjectName)
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
