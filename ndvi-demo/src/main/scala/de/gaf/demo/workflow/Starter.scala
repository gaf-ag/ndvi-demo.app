/*
 * Copyright (c) 2016 GAF AG, Munich, Germany. All rights reserved.
 */

package de.gaf.demo.workflow

import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.Level
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.runtime.universe

/**
  * Starts the Spark Workflow as defined in spark.workflowconfig.
  */
object Starter extends LazyLogging {
  val sparkLogLevel = Level.DEBUG
  val sparkDefaultMaster = "local[*]"
  val sparkDefaultAppName = "Default-App"

  /**
    * Parses arguments, configures and runs SparkContext.
    *
    * @param args Program arguments.
    */
  def main(args: Array[String]) {

    val sparkConf = createSparkConf()
    implicit val sc = new SparkContext(sparkConf)

    sc.setLogLevel("INFO")

    try
      createWorkflowInstance("de.gaf.demo.apps.NdviWorkflow").runSparkProcess
    catch {
      case e: Exception => logger.error(e.getMessage, e)
    } finally
      sc.stop()
  }

  def createSparkConf(): SparkConf =
    new SparkConf()
      .setIfMissing("spark.master", sparkDefaultMaster)
      .setIfMissing("spark.app.name", sparkDefaultAppName)
      .set("spark.scheduler.mode", "FIFO")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", classOf[de.gaf.demo.workflow.RicKryoRegistrator].getName)
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.kryoserializer.buffer.max", "512m")

  def createWorkflowInstance(classname: String): Workflow = {
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(classname)
    runtimeMirror.reflectModule(module).instance match {
      case w: Workflow => w
      case _ => throw new ClassCastException("Class " + classname + " is not a Workflow")
    }
  }
 }
