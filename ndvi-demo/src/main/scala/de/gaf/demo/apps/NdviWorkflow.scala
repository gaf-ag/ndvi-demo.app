/*
 * Copyright (c) 2016 GAF AG, Munich, Germany. All rights reserved.
 */

package de.gaf.demo.apps

import com.typesafe.scalalogging.LazyLogging
import de.gaf.demo.algo.RicException
import de.gaf.demo.algo.index.Ndvi
import de.gaf.demo.io.GeotiffRddLoader
import de.gaf.demo.workflow.Workflow
import geotrellis.raster.{MultibandTile, Raster, Tile}
import geotrellis.spark.tiling.FloatingLayoutScheme
import geotrellis.spark.{SpatialKey, TileLayerMetadata, TileLayerRDD, _}
import geotrellis.vector.ProjectedExtent
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.Seq
import scala.collection.parallel.{ForkJoinTaskSupport, ParSeq}
import scala.concurrent.forkjoin.ForkJoinPool

/**
  * The Ndvi Workflow reads in a multiband GeoTiff and writes out a singleband GeoTiff
  * with NDVI values.
  *
  */
object NdviWorkflow extends Workflow with LazyLogging {

  /**
    * Executes the NDVI workflow.
    *
    * @param sc     Connection to Spark.
    */
  def runSparkProcess()(implicit sc: SparkContext): Unit = {

    logger.info("NDVI application is starting Spark process...")

    val scenes: Seq[String] = List(
      "s3://landsat-pds/L8/020/015/LC80200152016235LGN00/LC80200152016235LGN00",
      "s3://landsat-pds/L8/173/070/LC81730702016235LGN00/LC81730702016235LGN00",
      "s3://landsat-pds/L8/173/065/LC81730652016235LGN00/LC81730652016235LGN00",
      "s3://landsat-pds/L8/093/083/LC80930832016235LGN00/LC80930832016235LGN00",
      "s3://landsat-pds/L8/036/026/LC80360262016235LGN00/LC80360262016235LGN00",
      "s3://landsat-pds/L8/173/034/LC81730342016235LGN00/LC81730342016235LGN00",
      "s3://landsat-pds/L8/084/024/LC80840242016235LGN00/LC80840242016235LGN00",
      "s3://landsat-pds/L8/020/045/LC80200452016235LGN00/LC80200452016235LGN00",
      "s3://landsat-pds/L8/125/064/LC81250642016235LGN00/LC81250642016235LGN00",
      "s3://landsat-pds/L8/141/017/LC81410172016235LGN00/LC81410172016235LGN00",
      "s3://landsat-pds/L8/004/048/LC80040482016235LGN00/LC80040482016235LGN00",
      "s3://landsat-pds/L8/221/070/LC82210702016235LGN00/LC82210702016235LGN00",
      "s3://landsat-pds/L8/189/026/LC81890262016235LGN00/LC81890262016235LGN00",
      "s3://landsat-pds/L8/052/086/LC80520862016235LGN00/LC80520862016235LGN00",
      "s3://landsat-pds/L8/189/025/LC81890252016235LGN00/LC81890252016235LGN00",
      "s3://landsat-pds/L8/173/073/LC81730732016235LGN00/LC81730732016235LGN00",
      "s3://landsat-pds/L8/173/051/LC81730512016235LGN00/LC81730512016235LGN00",
      "s3://landsat-pds/L8/141/048/LC81410482016235LGN00/LC81410482016235LGN00",
      "s3://landsat-pds/L8/125/042/LC81250422016235LGN00/LC81250422016235LGN00",
      "s3://landsat-pds/L8/221/082/LC82210822016235LGN00/LC82210822016235LGN00",
      "s3://landsat-pds/L8/084/057/LC80840572016235LGN00/LC80840572016235LGN00",
      "s3://landsat-pds/L8/020/044/LC80200442016235LGN00/LC80200442016235LGN00",
      "s3://landsat-pds/L8/036/019/LC80360192016235LGN00/LC80360192016235LGN00"
    )

    val input: ParSeq[(String, RDD[(ProjectedExtent, MultibandTile)])] =
      scenes.map { (scene: String) =>
        (scene, GeotiffRddLoader.tileAndPartitionMultiband(List(scene), GeotiffRddLoader.getRasterFromS3))
      }.par

    input.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(sc.defaultParallelism))

    if (input.isEmpty) logger.warn("Application input is empty. Nothing will be processed.")
    input.foreach { case (sceneName, sceneRdd) =>

      try {

        sceneRdd.persist(StorageLevel.MEMORY_AND_DISK)
        logger.info(s"Input GeoTiff RDD for scene $sceneName is set.")

        // Map bands to NDVI
        val ndvi: RDD[(ProjectedExtent, Tile)] = sceneRdd.mapValues(Ndvi(_))

        // Call some actions on the result
        val metadata: TileLayerMetadata[SpatialKey] = TileLayerMetadata.fromRdd(
          ndvi, FloatingLayoutScheme(512))._2
        val tileLayerRDD: TileLayerRDD[SpatialKey] = TileLayerRDD[SpatialKey](
          ndvi.tileToLayout(metadata), metadata)
        val raster: Raster[Tile] = tileLayerRDD.stitch

        sceneRdd.unpersist()
        logger.info(s"Unpersisted RDD for scene $sceneName.")

        logger.info(s"NDVI Spark process for scene $sceneName is done.")
      } catch {
        case ricException: RicException => logger.error(ricException.getMessage, ricException)
      }
    }
  }
}
