package org.grape

import cats.effect.{ExitCode, IO, IOApp}
import org.apache.spark.sql.functions.{expr, first}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

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
    val session = initSparkSession
    import org.apache.log4j.{Level, Logger}
    import session.implicits._

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    //Datasets

    // add host_name, ip_address
    val ge_device_inventory = Seq(
      (
        "00005B",
        "mycompany.com",
        "100.25.9.1",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "00005D",
        "mycompany.com",
        "100.25.9.2",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "00005F",
        "mycompany.com",
        "100.25.9.3",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "000060",
        "mycompany.com",
        "100.25.9.4",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "000061",
        "mycompany.com",
        "100.25.9.5",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      )
    ).toDF(
      "ge_device_inventory_entity_id",
      "ge_device_inventory_host_name",
      "ge_device_inventory_ip",
      "ge_device_inventory_manufacturer",
      "ge_device_inventory_domain",
      "ge_device_inventory_dt"
    )

    // add host_name, ip_address
    val ge_device_record = Seq(
      (
        "00005B",
        "service_now_0",
        "r_service_now_asset",
        "mycompany.com",
        "100.25.9.1",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "00005B",
        "qualys_0",
        "r_qualys_host",
        "mycompany.com",
        "100.25.9.1",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "00005D",
        "service_now_1",
        "r_service_now_asset",
        "mycompany.com",
        "100.25.9.2",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "00005D",
        "qualys_1",
        "r_qualys_host",
        "mycompany.com",
        "100.25.9.2",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "00005F",
        "service_now_2",
        "r_service_now_asset",
        "mycompany.com",
        "100.25.9.3",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "00005F",
        "qualys_2",
        "r_qualys_host",
        "mycompany.com",
        "100.25.9.3",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "000060",
        "service_now_3",
        "r_service_now_asset",
        "mycompany.com",
        "100.25.9.3",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "000060",
        "qualys_3",
        "r_qualys_host",
        "mycompany.com",
        "100.25.9.3",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "000061",
        "service_now_4",
        "r_service_now_asset",
        "mycompany.com",
        "100.25.9.3",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      ),
      (
        "000061",
        "qualys_4",
        "r_qualys_host",
        "mycompany.com",
        "100.25.9.3",
        "Dell Inc.",
        "AMERICAS.MYCOMPANY.COM",
        "20200126"
      )
    ).toDF(
      "ge_device_record_entity_id",
      "ge_device_record_raw_id",
      "ge_device_record_raw_table_name",
      "ge_device_record_host_name",
      "ge_device_record_ip",
      "ge_device_record_manufacturer",
      "ge_device_record_domain",
      "ge_device_record_dt"
    )

    println("\n\n\n\n\n\n\n\n\n### Device Inventory table [Hive table]")
    println(
      "------------------------------------------------- ge_device_inventory -------------------------------------------------"
    )
    ge_device_inventory.show()

    val r_service_now_asset = Seq(
      (
        "service_now_0",
        "bos-10019.americas.mycompany.com",
        "americas.mycompany.com",
        "ASST20019S",
        "Windows Server 2012 Standard Edition",
        "NULL",
        "NULL"
      ),
      (
        "service_now_1",
        "bos-10020.americas.mycompany.com",
        "americas.mycompany.com",
        "ASST20020S",
        "Windows 10",
        "NULL",
        "NULL"
      ),
      (
        "service_now_2",
        "bos-10021.americas.mycompany.com",
        "americas.mycompany.com",
        "ASST20021S",
        "Windows Server 2012 Standard Edition",
        "NULL",
        "NULL"
      ),
      (
        "service_now_3",
        "bos-10022.americas.mycompany.com",
        "americas.mycompany.com",
        "ASST20022S",
        "Windows Server 2012 Standard Edition",
        "NULL",
        "NULL"
      ),
      (
        "service_now_4",
        "bos-10023.americas.mycompany.com",
        "americas.mycompany.com",
        "ASST20023S",
        "NULL",
        "NULL",
        "NULL"
      )
    ).toDF(
      "r_service_now_asset_raw_id",
      "r_service_now_asset_fqdn",
      "r_service_now_asset_dns_domain",
      "r_service_now_asset_serial_number",
      "r_service_now_asset_os",
      "r_service_now_asset_sys_class_name",
      "r_service_now_asset_name"
    )

    println("\n\n### Service Now device raw table [Hive table]")
    println(
      "------------------------------------------------- r_service_now_asset -------------------------------------------------"
    )
    r_service_now_asset.show()

    val r_qualys_host = Seq(
      ("qualys_0", "Server", "bos-10062"),
      ("qualys_1", "Virtual Machine Instance", "MSSQLSERVER"),
      ("qualys_2", "Storage Device", "SQL_SERVER_DB_1"),
      ("qualys_3", "Server", "MSSQLSERVER1"),
      ("qualys_4", "Server", "MSSQLSERVER2")
    ).toDF(
      "r_qualys_host_raw_id",
      "r_qualys_host_sys_class_name",
      "r_qualys_host_name"
    )

    println("\n\n### Qualys device raw table [Hive table]")
    println(
      "------------------------------------------------- r_qualys_host -------------------------------------------------"
    )
    r_qualys_host.show()

    val originated_refs = ge_device_record
      .select(
        $"ge_device_record_entity_id",
        $"ge_device_record_raw_table_name",
        $"ge_device_record_raw_id"
      )
      .toDF(
        "originated_refs_inventory_id",
        "originated_refs_originated_table",
        "originated_refs_originated_row_id"
      )

    println(
      "\n\n### The table which stores [Inventory entity <--> Raw records] relationship [future Hive table]"
    )
    println(
      "------------------------------------------------- originated_refs -------------------------------------------------"
    )
    originated_refs.show()

    //Comes from database with user preferences
    val userPreferences = UserPref(
      Map(
        "DeviceEntity" -> List(
          BCAttribute(
            "device_type",
            """
              |case
              | when r_service_now_asset_os in ("OSX","Windows 10","Windows 7", "Windows XP","Windows Vista","Windows 8") or r_service_now_asset_os = "Ubuntu Desktop" then "User Device"
              | when r_service_now_asset_os in ("OSX","Windows 10","Windows 7", "Windows XP","Windows Vista","Windows 8") or r_service_now_asset_os = "Ubuntu Desktop" then "User Device"
              | when r_service_now_asset_os rlike "(?i)(cisco)|(switch)|(fabric)|(f5 networks)|(netgear)|(bluecoat)|(emc)|(iolan)|(serial console)" then "Network"
              | when r_service_now_asset_os rlike "(?i)(server)|(enterprise linux)|(esxi)" then "Server"
              | else "Other"
              |end
              |""".stripMargin,
            List("ge_device_inventory", "r_service_now_asset", "r_qualys_host")
          ),
          BCAttribute(
            "criticality",
            """
              |case
              | when device_type='User Device' then 'High'
              | when device_type='Server' then 'Medium'
              | else "Undefined"
              |end
              |""".stripMargin,
            Nil
          ),
          BCAttribute(
            "bu",
            """
              |case
              | when r_service_now_asset_serial_number in ("ASST20019S", "ASST20020S") and r_qualys_host_sys_class_name in ("Server", "Storage Device") then "Designers"
              | else "Engineers"
              |end
              |""".stripMargin,
            List("ge_device_inventory", "r_service_now_asset", "r_qualys_host")
          ),
          BCAttribute(
            "class_name",
            """
              |case
              | when r_service_now_asset_sys_class_name != "NULL" then r_service_now_asset_sys_class_name
              | when r_qualys_host_sys_class_name != "NULL" then r_qualys_host_sys_class_name
              | else "Unknown"
              |end
              |""".stripMargin,
            List("ge_device_inventory", "r_service_now_asset", "r_qualys_host")
          )
        )
      )
    )

    //Imitates reading from Hive
    val hive = Map(
      "ge_device_inventory" -> ge_device_inventory,
      "r_service_now_asset" -> r_service_now_asset,
      "r_qualys_host" -> r_qualys_host,
      "originated_refs" -> originated_refs
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
