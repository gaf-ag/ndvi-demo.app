/*
 * Copyright (c) 2016 GAF AG, Munich, Germany. All rights reserved.
 */

package de.gaf.demo.workflow

import org.apache.spark.SparkContext

/**
  * A Workflow for image processing.
  */
trait Workflow {
  def runSparkProcess()(implicit sc: SparkContext)
}
