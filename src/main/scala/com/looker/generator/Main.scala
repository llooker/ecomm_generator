package com.looker.generator

import com.looker.generator.DataOutput.DataOutputTrait
import com.looker.utils.FileUtils


import java.io.{FileWriter, File}

import org.joda.time.{Seconds, DateTime}

object Main {
  def main(args: Array[String]) {
    val everything = args.length == 0

    val runMode = if (everything) {
      0
    } else if (args.length > 0 && args(0).startsWith("generate")) {
      1
    } else if (args.length > 0 && args(0).startsWith("schema")) {
      2
    } else {
      3
    }

    if (runMode == 0 || runMode == 1) {

      val mode : String = if (everything) {
        "all"
      } else {
        args(0).toLowerCase.split(":").toList match {
          case _ :: Nil => "all"
          case _ :: x :: Nil => x
          case _ => "error"
        }
      }

      if (! Set("all", "data", "products", "distribution_centers")(mode)) {
        println(s"Invalid generation mode: $mode")
        System.exit(1)
      }

      var time = DateTime.now
      val outputDirectory = "data"

      // create output directory if it doesn't exist
      new File(outputDirectory).mkdirs

      if (mode == "all" || mode == "data") {
        FileUtils.removeDataFiles(outputDirectory)

        Generator.generate(outputDirectory, 4, 1000)
        var difference = Seconds.secondsBetween(time, DateTime.now).getSeconds
        println(s"Generation of data took $difference second(s)")
      }

      if (mode == "all" || mode == "distribution_centers") {
        Generator.generateDistributionCenterData(outputDirectory)
      }

      if (mode == "all" || mode == "products") {
        Generator.generateProductData(outputDirectory)
      }

    }
    if (runMode == 0 || runMode == 2) {
      val dialect = if (everything) {
        "MySQL"
      } else {
        args(0).toLowerCase.split(":").toList match {
          case _ :: Nil               => "MySQL"
          case _ :: "mysql"    :: Nil => "MySQL"
          case _ :: "redshift" :: Nil => "Redshift"
          case _ :: "bigquery" :: Nil => "BigQuery"
          case _ :: "sqlite"   :: Nil => "SQLite"
          case _                      => "error"
        }
      }
      val dataDirectory = "schema"

      new File(dataDirectory).mkdirs
      FileUtils.removeDataFiles(dataDirectory)

      val classes: Array[DataOutputTrait] = Array(
        DataOutput.Event,
        DataOutput.InventoryItem,
        DataOutput.OrderItem,
        DataOutput.Product,
        DataOutput.User,
        DataOutput.DistributionCenter
      )

      classes.foreach( e => {
        val fileType = if (dialect == "BigQuery") {"json"} else {"sql"}
        val schemaDirectory = s"$dataDirectory/${dialect.toLowerCase}"
        new File(schemaDirectory).mkdirs

        val filename = s"$schemaDirectory/${e.tableName.toLowerCase}.$fileType"
        val fileWriter = new FileWriter(filename)
        fileWriter.write(e.toDDL(dialect))
        fileWriter.close
      })
    }
  }
}
