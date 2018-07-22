/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import com.netflix.iceberg.parquet.ParquetMetrics
import org.apache.parquet.hadoop.ParquetFileReader
import scala.collection.JavaConverters._

import org.apache.hadoop.fs.PathFilter

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition

import com.netflix.iceberg.DataFile
import com.netflix.iceberg.DataFiles
import com.netflix.iceberg.Metrics
import com.netflix.iceberg.PartitionSpec
import com.netflix.iceberg.spark.hacks.Hive

import collection.JavaConversions._

object SparkTableUtil {
  /**
   * Returns a DataFrame with a row for each partition in the table.
   *
   * The DataFrame has 3 columns, partition key (a=1/b=2), partition location, and format
   * (avro or parquet).
   *
   * @param spark a Spark session
   * @param table a table name and (optional) database
   * @return a DataFrame of the table's partitions
   */
  def partitionDF(spark: SparkSession, table: String): DataFrame = {
    import spark.implicits._

    val partitions: Seq[(Map[String, String], Option[String], Option[String])] =
      Hive.partitions(spark, table).map { p: CatalogTablePartition =>
        (p.spec, p.storage.locationUri.map(_.toString), p.storage.serde)
      }

    partitions.toDF("partition", "uri", "format")
  }

  /**
   * Returns the data files in a partition by listing the partition location.
   *
   * For Parquet partitions, this will read metrics from the file footer. For Avro partitions,
   * metrics are set to null.
   *
   * @param partition partition key, e.g., "a=1/b=2"
   * @param uri partition location URI
   * @param format partition format, avro or parquet
   * @return a seq of [[SparkDataFile]]
   */
  def listPartition(
      partition: Map[String, String],
      uri: String,
      format: String): Seq[SparkDataFile] = {
    if (format.contains("avro")) {
      listAvroPartition(partition, uri)
    } else if (format.contains("parquet")) {
      listParquetPartition(partition, uri)
    } else {
      throw new UnsupportedOperationException(s"Unknown partition format: $format")
    }
  }

  /**
   * Case class representing a data file.
   */
  case class SparkDataFile(
      path: String,
      partition: collection.Map[String, String],
      format: String,
      fileSize: Long,
      rowGroupSize: Long,
      rowCount: Long,
      columnSizes: java.util.Map[Integer, java.lang.Long],
      valueCounts: java.util.Map[Integer, java.lang.Long],
      nullValueCounts: java.util.Map[Integer, java.lang.Long],
      lowerBounds: java.util.Map[Integer, java.nio.ByteBuffer],
      upperBounds: java.util.Map[Integer, java.nio.ByteBuffer]
    ) {

    /**
     * Convert this to a [[DataFile]] that can be added to a [[com.netflix.iceberg.Table]].
     *
     * @param spec a [[PartitionSpec]] that will be used to parse the partition key
     * @return a [[DataFile]] that can be passed to [[com.netflix.iceberg.AppendFiles]]
     */
    def toDataFile(spec: PartitionSpec): DataFile = {
      // values are strings, so pass a path to let the builder coerce to the right types
      val partitionKey = spec.fields.asScala.map(_.name).map { name =>
        s"$name=${partition(name)}"
      }.mkString("/")

      DataFiles.builder(spec)
          .withPath(path)
          .withFormat(format)
          .withPartitionPath(partitionKey)
          .withFileSizeInBytes(fileSize)
          .withBlockSizeInBytes(rowGroupSize)
          .withMetrics(new Metrics(rowCount,
            mapAsJavaMap(columnSizes),
            mapAsJavaMap(valueCounts),
            mapAsJavaMap(nullValueCounts),
            mapAsJavaMap(lowerBounds),
            mapAsJavaMap(upperBounds)))
          .build()
    }
  }

  private object HiddenPathFilter extends PathFilter {
    override def accept(p: Path): Boolean = {
      !p.getName.startsWith("_") && !p.getName.startsWith(".")
    }
  }

  private def listAvroPartition(
      partitionPath: Map[String, String],
      partitionUri: String): Seq[SparkDataFile] = {
    val conf = new Configuration()
    val partition = new Path(partitionUri)
    val fs = partition.getFileSystem(conf)

    fs.listStatus(partition, HiddenPathFilter).filter(_.isFile).map { stat =>
      SparkDataFile(
        stat.getPath.toString,
        partitionPath, "avro", stat.getLen,
        stat.getBlockSize,
        -1,
        null,
        null,
        null,
        null,
        null)
    }
  }

  //noinspection ScalaDeprecation
  private def listParquetPartition(
      partitionPath: Map[String, String],
      partitionUri: String): Seq[SparkDataFile] = {
    val conf = new Configuration()
    val partition = new Path(partitionUri)
    val fs = partition.getFileSystem(conf)

    fs.listStatus(partition, HiddenPathFilter).filter(_.isFile).map { stat =>
      val metrics = ParquetMetrics.fromMetadata(ParquetFileReader.readFooter(conf, stat))

      SparkDataFile(
        stat.getPath.toString,
        partitionPath, "parquet", stat.getLen,
        stat.getBlockSize,
        metrics.recordCount,
        metrics.columnSizes,
        metrics.valueCounts,
        metrics.nullValueCounts,
        metrics.lowerBounds,
        metrics.upperBounds)
    }
  }
}

