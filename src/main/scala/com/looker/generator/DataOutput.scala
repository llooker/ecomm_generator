package com.looker.generator

import com.looker.utils.TimeFormatter
import org.joda.time.{DateTime, LocalDate}
import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.Future

object DataOutput {

  implicit val nullString : String = ""

  object ColumnType extends Enumeration {
    val Integer, Double, Timestamp, String = Value
  }

  case class SchemaColumn(columnName : String, columnType : ColumnType.Value, length : Int = 0) {
    def typeString(dialect : String) : String  = columnType match {
      case ColumnType.Integer   => "INTEGER"
      case ColumnType.Timestamp => dialect.toLowerCase match {
        case "mysql"  => "TIMESTAMP NULL"
        case "sqlite" => "TEXT"
        case _        => "TIMESTAMP"
      }
      case ColumnType.String     => dialect.toLowerCase match {
        case "bigquery" => "STRING"
        case "sqlite"   => "TEXT"
        case _          => s"VARCHAR($length)"
      }
      case ColumnType.Double     => dialect.toLowerCase match {
        case "mysql"    => "DOUBLE"
        case "bigquery" => "FLOAT"
        case "sqlite"   => "REAL"
        case _          => "DOUBLE PRECISION"
      }
    }
  }

  case class Schema(columns : Array[SchemaColumn], sortKey : String, distStyle : Option[String] = None, distKey : Option[String] = None) {
    def toDDL(table_name: String, dialect : String, dropIfExists : Boolean = true) : String = {
      if (dialect == "BigQuery") {
        s"[\n${columns.map(e => s"""  {"name":"${e.columnName}", "type":"${e.typeString(dialect)}", "mode":"nullable"}""").mkString(", \n")}\n]"
      } else {
        val extra = dialect.toLowerCase match {
          case "redshift" => {
            var modifiers: List[String] = List(s"SORTKEY ($sortKey)")
            if (distStyle.nonEmpty) {modifiers = s"DISTSTYLE ${distStyle.get}" :: modifiers}
            if (distKey.nonEmpty)   {modifiers  =s"DISTKEY (${distKey.get})"   :: modifiers}

            modifiers.mkString(" ")
          }
          case _          => ""
        }

        s"""
          |DROP TABLE IF EXISTS $table_name;
          |
          |CREATE TABLE $table_name (
          ${columns.map(e => s"|  ${e.columnName} ${e.typeString(dialect)}").mkString(", \n")}
          |) $extra;
        """.stripMargin.trim
      }
    }
  }

  trait DataOutputTrait {
    val tableName : String

    def toDDL(dialect : String) = schema.toDDL(tableName, dialect)

    def schema : Schema
  }

  abstract class DataOutputObject {
    def futureString : Future[String]
  }

  object OrderItem extends DataOutputTrait {
    val tableName = "ORDER_ITEMS"

    def schema : Schema = {
      Schema(Array(
        SchemaColumn("ID",                ColumnType.Integer),
        SchemaColumn("ORDER_ID",          ColumnType.Integer),
        SchemaColumn("USER_ID",           ColumnType.Integer),
        SchemaColumn("INVENTORY_ITEM_ID", ColumnType.Integer),
        SchemaColumn("SALE_PRICE",        ColumnType.Double),
        SchemaColumn("STATUS",            ColumnType.String, 30),
        SchemaColumn("CREATED_AT",        ColumnType.Timestamp),
        SchemaColumn("RETURNED_AT",       ColumnType.Timestamp),
        SchemaColumn("SHIPPED_AT",        ColumnType.Timestamp),
        SchemaColumn("DELIVERED_AT",      ColumnType.Timestamp)
      ), sortKey = "CREATED_AT", distKey = Some("USER_ID"))
    }
  }

  case class OrderItem(
    id: Future[Int],
    orderId: Future[Int],
    userId: Future[Int],
    salePrice: Double,
    inventoryItemId: Future[Int],
    orderStatus: String,
    createdAt: DateTime,
    returnedAt: Option[DateTime],
    shippedAt: Option[LocalDate],
    deliveredAt: Option[LocalDate]
  ) extends DataOutputObject {

    def futureString = {
      val returnedAtString  = TimeFormatter.formatNullable(returnedAt)
      val shippedAtString   = TimeFormatter.formatNullable(shippedAt)
      val deliveredAtString = TimeFormatter.formatNullable(deliveredAt)
      for {
        oiid <- id
        oid <- orderId
        uid <- userId
        iiid <- inventoryItemId
      } yield List(
        oiid,
        oid,
        uid,
        iiid,
        salePrice,
        orderStatus,
        TimeFormatter.format(createdAt),
        TimeFormatter.formatNullable(returnedAt),
        TimeFormatter.formatNullable(shippedAt),
        TimeFormatter.formatNullable(deliveredAt)
      ).mkString(",")
    }
  }

  object InventoryItem extends DataOutputTrait {
    val tableName = "INVENTORY_ITEMS"

    def schema : Schema = {
      Schema(Array(
        SchemaColumn("ID",                             ColumnType.Integer),
        SchemaColumn("PRODUCT_ID",                     ColumnType.Integer),
        SchemaColumn("CREATED_AT",                     ColumnType.Timestamp),
        SchemaColumn("SOLD_AT",                        ColumnType.Timestamp),
        SchemaColumn("COST",                           ColumnType.Double),
        SchemaColumn("PRODUCT_CATEGORY",               ColumnType.String, 255),
        SchemaColumn("PRODUCT_NAME",                   ColumnType.String, 255),
        SchemaColumn("PRODUCT_BRAND",                  ColumnType.String, 255),
        SchemaColumn("PRODUCT_RETAIL_PRICE",           ColumnType.Double),
        SchemaColumn("PRODUCT_DEPARTMENT",             ColumnType.String, 255),
        SchemaColumn("PRODUCT_SKU",                    ColumnType.String, 64),
        SchemaColumn("PRODUCT_DISTRIBUTION_CENTER_ID", ColumnType.Integer)
      ), sortKey = "CREATED_AT", distKey = Some("PRODUCT_ID"))
    }
  }

  case class InventoryItem (
    id: Future[Int],
    product: com.looker.generator.Product,
    createdAt: LocalDate,
    soldAt: Option[DateTime]
  ) extends DataOutputObject {

    def futureString = {
      for {
        id <- id
      } yield List(
        id,
        product.id,
        TimeFormatter.format(createdAt),
        TimeFormatter.formatNullable(soldAt),
        product.cost,
        product.category.name,
        product.name,
        product.brand.name,
        product.retailPrice,
        product.category.department.toString,
        product.sku,
        product.distributionCenter.id
      ).mkString(",")
    }
  }

  object Event extends DataOutputTrait {
    val tableName = "EVENTS"

    def schema = Schema(Array(
      SchemaColumn("ID",              ColumnType.Integer),
      SchemaColumn("SEQUENCE_NUMBER", ColumnType.Integer),
      SchemaColumn("SESSION_ID",      ColumnType.String, 255),
      SchemaColumn("CREATED_AT",      ColumnType.Timestamp),
      SchemaColumn("IP_ADDRESS",      ColumnType.String, 16),
      SchemaColumn("CITY",            ColumnType.String, 255),
      SchemaColumn("STATE",           ColumnType.String, 255),
      SchemaColumn("COUNTRY",         ColumnType.String, 4),
      SchemaColumn("ZIP",             ColumnType.String, 16),
      SchemaColumn("LATITUDE",        ColumnType.Double),
      SchemaColumn("LONGITUDE",            ColumnType.Double),
      SchemaColumn("OS",              ColumnType.String, 30),
      SchemaColumn("BROWSER",         ColumnType.String, 30),
      SchemaColumn("TRAFFIC_SOURCE",  ColumnType.String, 30),
      SchemaColumn("USER_ID",         ColumnType.Integer),
      SchemaColumn("URI",             ColumnType.String, 255),
      SchemaColumn("EVENT_TYPE",      ColumnType.String, 30)

    ), sortKey = "CREATED_AT", distKey = Some("SESSION_ID"))
  }

  case class Event(session : Session,  event: com.looker.generator.Event, sessionId: String, sequenceNumber : Int) extends DataOutputObject {

    def futureString : Future[String] = {
      val location = session.userData.location
      for {
        id <- event.id.get
        uid <- event.userId.getOrElse(Future(nullString))
      } yield List(
        id,
        sequenceNumber,
        sessionId,
        TimeFormatter.format(event.time),
        location.ip,
        location.city,
        location.state,
        location.country,
        location.zip,
        location.lat,
        location.long,
        session.userData.os,
        session.userData.browser,
        session.trafficSource,
        uid,
        event.uri,
        event.eventType
      ).mkString(",")
    }
  }

  object User extends DataOutputTrait{
    def apply(day: DateTime, d: UserData): User = {
      val name = UserManager.getName(d.gender)
      val email = UserManager.getEmail(name._1, name._2)
      val l = d.location

      User(d.optionalId.get, name._1, name._2, email, d.age, l.city, l.state, l.country, l.zip, l.lat, l.long, d.gender, day, d.trafficSource)
    }

    val tableName = "USERS"

    def schema : Schema = Schema(Array(
      SchemaColumn("ID",             ColumnType.Integer),
      SchemaColumn("FIRST_NAME",     ColumnType.String, 255),
      SchemaColumn("LAST_NAME",      ColumnType.String, 255),
      SchemaColumn("EMAIL",          ColumnType.String, 255),
      SchemaColumn("AGE",            ColumnType.Integer),
      SchemaColumn("CITY",           ColumnType.String, 255),
      SchemaColumn("STATE",          ColumnType.String, 255),
      SchemaColumn("COUNTRY",        ColumnType.String, 4),
      SchemaColumn("ZIP",            ColumnType.String, 16),
      SchemaColumn("LATITUDE",       ColumnType.Double),
      SchemaColumn("LONGITUDE",      ColumnType.Double),
      SchemaColumn("GENDER",         ColumnType.String, 6),
      SchemaColumn("CREATED_AT",     ColumnType.Timestamp),
      SchemaColumn("TRAFFIC_SOURCE", ColumnType.String, 16)
    ), sortKey = "ID", distKey = Some("ID"))
  }

  case class User(
    id: Future[Int],
    firstName: String,
    lastName: String,
    email : String,
    age: Int,
    city: String,
    state: String,
    country: String,
    zip: String,
    lat: Double,
    long: Double,
    gender: Gender.Value,
    createdAt: DateTime,
    trafficSource: String
  ) extends DataOutputObject {

    def futureString : Future[String] =  {
      for {
        realized_id <- id
      } yield List(
        realized_id,
        firstName,
        lastName,
        email,
        age,
        city,
        state,
        country,
        zip,
        lat,
        long,
        gender,
        TimeFormatter.format(createdAt),
        trafficSource
      ).mkString(",")
    }
  }

  object Product extends DataOutputTrait {
    val tableName = "PRODUCTS"

    def schema = Schema(Array(
      SchemaColumn("ID",                     ColumnType.Integer),
      SchemaColumn("COST",                   ColumnType.Double),
      SchemaColumn("CATEGORY",               ColumnType.String, 255),
      SchemaColumn("NAME",                   ColumnType.String, 255),
      SchemaColumn("BRAND",                  ColumnType.String, 255),
      SchemaColumn("RETAIL_PRICE",           ColumnType.Double),
      SchemaColumn("DEPARTMENT",             ColumnType.String, 255),
      SchemaColumn("SKU",                    ColumnType.String, 64),
      SchemaColumn("DISTRIBUTION_CENTER_ID", ColumnType.Integer)
    ), sortKey = "ID", distKey = Some("ID"))
  }

  case class Product(
    product : com.looker.generator.Product
  ) extends DataOutputObject {

    def futureString: Future[String] = {
      Future(List(
        product.id,
        product.cost,
        product.category.name,
        product.name,
        product.brand.name,
        product.retailPrice,
        product.category.department.toString,
        product.sku,
        product.distributionCenter.id
      ).mkString(","))
    }
  }

  object DistributionCenter extends DataOutputTrait {
    val tableName = "DISTRIBUTION_CENTERS"

    def schema = Schema(Array(
      SchemaColumn("ID", ColumnType.Integer),
      SchemaColumn("NAME", ColumnType.String, 255),
      SchemaColumn("LATITUDE", ColumnType.Double),
      SchemaColumn("LONGITUDE", ColumnType.Double)
    ), sortKey = "ID", distStyle = Some("ALL"))
  }

  case class DistributionCenter(id : Int, name : String, lat : Double, long : Double) {
    def futureString : Future[String] = {
      Future(List(id, name, lat, long).mkString(","))
    }
  }
}
