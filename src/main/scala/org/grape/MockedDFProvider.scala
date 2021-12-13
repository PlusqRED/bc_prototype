package org.grape

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.grape.Prototype.{BCAttribute, UserPref}

object MockedDFProvider {
  def mockedHiveTableDFMap(
      ge_device_inventory: DataFrame,
      r_service_now_asset: DataFrame,
      r_qualys_host: DataFrame,
      originated_refs: DataFrame
  ): Map[String, DataFrame] = {
    Map(
      "ge_device_inventory" -> ge_device_inventory,
      "r_service_now_asset" -> r_service_now_asset,
      "r_qualys_host" -> r_qualys_host,
      "originated_refs" -> originated_refs
    )
  }

  def mappedUserPreferences: UserPref = {
    UserPref(
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
  }

  def originated_refsDF(
      ge_device_record: DataFrame
  )(implicit session: SparkSession): DataFrame = {
    import session.implicits._
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
    originated_refs
  }

  def r_qualys_hostDF(implicit session: SparkSession): DataFrame = {
    import session.implicits._
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
    r_qualys_host
  }

  def r_servicenow_assetDF(implicit
      session: SparkSession
  ): DataFrame = {
    import session.implicits._
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
    r_service_now_asset
  }

  def ge_device_recordDF(implicit
      session: SparkSession
  ): DataFrame = {
    import session.implicits._
    Seq(
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
  }

  def ge_device_inventoryDF(implicit
      session: SparkSession
  ): DataFrame = {
    import session.implicits._
    Seq(
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
  }

}
