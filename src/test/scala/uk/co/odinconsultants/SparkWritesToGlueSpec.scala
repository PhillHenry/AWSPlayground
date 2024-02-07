package uk.co.odinconsultants
import org.scalatest.GivenWhenThen
import uk.co.odinconsultants.SparkUtils.{CATALOG, DATABASE, TABLE_NAME, bucketName, spark, tableCreationSQL}
import uk.co.odinconsultants.documentation_utils.{SpecPretifier, TableNameFixture}

class SparkWritesToGlueSpec  extends SpecPretifier with GivenWhenThen with TableNameFixture {

  val fqn = s"$CATALOG.$DATABASE.${tableName.toLowerCase}"

  val s3Utils = new S3Utils(bucketName)

  val DIRECTORY = "iceberg"

  "Iceberg" should {
    "write to AWS Glue" in {
      val exits: Boolean = spark.catalog.tableExists(fqn)
      if (exits) spark.sql(s"DROP TABLE $fqn")
      Given(s"table $fqn does not exist")
      s3Utils.deleteObjectsMatching(s3Utils.getObjectNamesIn(bucketName), bucketName, DIRECTORY)
      And(s"bucket $bucketName had nothing under $DIRECTORY")
      val sql = tableCreationSQL(fqn)
      When(s"we execute:\n${formatSQL(sql)}")
      spark.sql(sql)
      Then(s"AWS Glue contains a table called $tableName")
      assert(spark.catalog.tableExists(fqn))
    }
  }

}
