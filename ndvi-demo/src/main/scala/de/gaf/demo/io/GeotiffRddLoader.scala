/*
 * Copyright (c) 2016 GAF AG, Munich, Germany. All rights reserved.
 */

package de.gaf.demo.io

import java.net.URI

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.retry.PredefinedRetryPolicies
import com.amazonaws.services.s3.AmazonS3Client
import com.typesafe.scalalogging.LazyLogging
import geotrellis.proj4.CRS
import geotrellis.raster.io.geotiff.SinglebandGeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import geotrellis.raster.split.Split
import geotrellis.raster.split.Split.Options
import geotrellis.raster.{MultibandTile, ProjectedRaster, Raster, TileLayout}
import geotrellis.vector.ProjectedExtent
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

object GeotiffRddLoader extends LazyLogging {

  val tileEdgeLength = 256
  val splitOptions: Options = Split.Options(cropped = false, extend = false)

  /**
    * Fetches a sequence of multi band images and puts them into a single RDD which is chunked
    * into 256 x 256 tiles and partitioned according to the settings in the config.
    *
    * @param images a sequence of images that will be put into a single RDD
    * @param source a fetcher function
    * @tparam ImageRef the image reference that the fetcher function can consume
    * @return an RDD that contains all the images and which is chunked and partitioned
    */
  def tileAndPartitionMultiband[ImageRef: ClassTag](images: Seq[ImageRef],
      source: ImageRef => ProjectedRaster[MultibandTile]): RDD[(ProjectedExtent, MultibandTile)] = {
    val rdd: RDD[(ProjectedExtent, MultibandTile)] = SparkContext.getOrCreate().parallelize(images,
      images.length).mapPartitions({ iter: Iterator[ImageRef] =>
      for {
        img: ImageRef <- iter
        ProjectedRaster(raster: Raster[MultibandTile], crs: CRS) = source(img)
        layoutCols: Int = math.ceil(raster.cols.toDouble / tileEdgeLength).toInt
        layoutRows: Int = math.ceil(raster.rows.toDouble / tileEdgeLength).toInt
        chunk: Raster[MultibandTile] <- raster.split(
          TileLayout(layoutCols, layoutRows, tileEdgeLength, tileEdgeLength), splitOptions)
      } yield ProjectedExtent(chunk.extent, crs) -> chunk.tile
    }, preservesPartitioning = true)

    val partitions: Int = images.length * 16
    if (partitions != rdd.getNumPartitions) rdd.repartition(partitions) else rdd
  }

  def getRasterFromS3(path: String): ProjectedRaster[MultibandTile] = {
    val bandsWanted = List("4", "5")
    val tifs: Seq[SinglebandGeoTiff] =
      for (band <- bandsWanted) yield {
        val uri = new URI(s"${path}_B${band.trim.toUpperCase}.TIF")
        val bucket = uri.getAuthority
        val prefix = uri.getPath.drop(1)
        val bytes = IOUtils.toByteArray(S3Client.default.getObject(bucket, prefix).getObjectContent)
        GeoTiffReader.readSingleband(bytes)
      }
    val tiles = tifs.map(_.tile).toArray
    val extent = tifs.head.extent
    val crs = tifs.head.crs
    ProjectedRaster(MultibandTile(tiles), extent, crs)
  }

  object S3Client {
    private def defaultConfiguration = {
      val config = new com.amazonaws.ClientConfiguration
      config.setMaxConnections(128)
      config.setMaxErrorRetry(16)
      config.setConnectionTimeout(100000)
      config.setSocketTimeout(100000)
      config.setRetryPolicy(PredefinedRetryPolicies.getDefaultRetryPolicyWithCustomMaxRetries(32))
      config
    }

    @transient lazy val default = new AmazonS3Client(new DefaultAWSCredentialsProviderChain(), defaultConfiguration)
  }
}
