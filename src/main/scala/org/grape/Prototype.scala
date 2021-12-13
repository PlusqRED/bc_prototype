package org.grape

import cats.effect.{ExitCode, IO, IOApp}
import org.apache.spark.sql.functions.{expr, first}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}
import org.grape.MockedDFProvider._

object Prototype extends IOApp {
  case class BCAttribute(name: String, rule: String, sources: List[String])
  case class UserPref(userPref: Map[String, List[BCAttribute]])

  def businessContextDataFrame(
      df: DataFrame,
      entityAttributes: List[BCAttribute]
  ): DataFrame = {
    entityAttributes
      .foldLeft(df)((df, bcAttribute) =>
        df.withColumn(bcAttribute.name, expr(bcAttribute.rule))
      )
      .select(
        "originated_refs_inventory_id",
        entityAttributes.map(_.name): _*
      )
  }

  def squash(df: DataFrame): List[Column] = {
    df.schema.names.map(functions.first(_, ignoreNulls = true)).toList
  }

  def sparkComputations(): Unit = {
    implicit val session: SparkSession = initSparkSession
    import org.apache.log4j.{Level, Logger}
    import session.implicits._

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //Datasets

    // add host_name, ip_address
    val ge_device_inventory = ge_device_inventoryDF

    // add host_name, ip_address
    val ge_device_record = ge_device_recordDF

    println("\n\n\n\n\n\n\n\n\n### Device Inventory table [Hive table]")
    println(
      "------------------------------------------------- ge_device_inventory -------------------------------------------------"
    )
    ge_device_inventory.show()

    val r_service_now_asset: DataFrame = r_servicenow_assetDF

    println("\n\n### Service Now device raw table [Hive table]")
    println(
      "------------------------------------------------- r_service_now_asset -------------------------------------------------"
    )
    r_service_now_asset.show()

    val r_qualys_host: DataFrame = r_qualys_hostDF

    println("\n\n### Qualys device raw table [Hive table]")
    println(
      "------------------------------------------------- r_qualys_host -------------------------------------------------"
    )
    r_qualys_host.show()

    val originated_refs: DataFrame = originated_refsDF(ge_device_record)

    println(
      "\n\n### The table which stores [Inventory entity <--> Raw records] relationship [future Hive table]"
    )
    println(
      "------------------------------------------------- originated_refs -------------------------------------------------"
    )
    originated_refs.show()

    //Comes from database with user preferences
    val userPreferences = mappedUserPreferences

    //Imitates reading from Hive
    val hive = mockedHiveTableDFMap(
      ge_device_inventory,
      r_service_now_asset,
      r_qualys_host,
      originated_refs
    )

    //distinct tables per business entity
    val businessEntityAndSources: Map[String, List[String]] =
      userPreferences.userPref.map { case (be, attributes) =>
        be -> attributes.flatMap(_.sources).distinct
      }

    businessEntityAndSources.foreach { case (_, tables) =>
      val mainTable: String = tables.head
      val mainTableWithRawRefs = hive(mainTable)
        .join(
          hive("originated_refs"),
          $"${mainTable}_entity_id" === $"originated_refs_inventory_id"
        )

      val df: DataFrame = tables.tail
        .foldLeft(mainTableWithRawRefs) { case (df, tableName) =>
          df.join(
            hive(tableName),
            $"${tableName}_raw_id" === $"originated_refs_originated_row_id",
            "full"
          ).drop(s"${tableName}_raw_id")
        }
        .drop(
          "originated_refs_originated_table",
          "originated_refs_originated_row_id",
          s"${mainTable}_entity_id"
        )

      val trueNames = df.schema.fieldNames

      val inventoryAndRawTables = df
        .groupBy("originated_refs_inventory_id")
        .agg(squash(df).head, squash(df).tail: _*)
        .drop("originated_refs_inventory_id")
        .toDF(trueNames: _*)

      println(
        "\n\n### The Spark Data Frame which contains Inventory Table + All Raw tables, which is mentioned in the particular Business Context Rule, joined together [Spark in-memory Data Frame]. " +
          "\nBusiness Context rules will be applied on this Spark Data Frame."
      )
      println(
        "\nThis is an example of the bu (business_unit) attribute evaluation rule: "
      )
      println(userPreferences.userPref("DeviceEntity")(2).rule)
      println(
        "------------------------------------------------- Inventory table JOIN Raw tables -------------------------------------------------"
      )
      inventoryAndRawTables.show()

      val entityAttributes: List[BCAttribute] =
        userPreferences.userPref("DeviceEntity")

      val bcDataFrame: DataFrame =
        businessContextDataFrame(inventoryAndRawTables, entityAttributes)
          .selectExpr(
            "originated_refs_inventory_id",
            s"stack(${entityAttributes.length}, ${entityAttributes
              .map(el => s"'${el.name}', ${el.name}")
              .mkString(",")})"
          )
          .withColumnRenamed(
            "col0",
            "column_name"
          )
          .withColumnRenamed(
            "col1",
            "value"
          )

      println(
        "\n\n### The key-value table which stores Business Context Data [future Hive table]"
      )
      println(
        "------------------------------------------------- Business Context Data Table -------------------------------------------------"
      )
      bcDataFrame.show()

      val preparedBC = bcDataFrame
        .groupBy("originated_refs_inventory_id")
        .pivot("column_name")
        .agg(first($"value"))

      val consumerReadyDataFrame: DataFrame = hive(mainTable)
        .join(
          preparedBC,
          $"originated_refs_inventory_id" === $"${mainTable}_entity_id"
        )
        .drop($"originated_refs_inventory_id")

      println(
        "\n\n### The prepared data [Inventory data + Business Context] to be consumed by third-party services (Metrics, Control Checks)."
      )
      println(
        "------------------------------------------------- Consumer Ready Data -------------------------------------------------"
      )
      consumerReadyDataFrame.show()
    }
  }

  private def initSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local[1]")
      .appName("Business_Context_Attributes_Prototype")
      .getOrCreate()
  }

  override def run(args: List[String]): IO[ExitCode] =
    IO(sparkComputations())
      .as(ExitCode.Success)
}
