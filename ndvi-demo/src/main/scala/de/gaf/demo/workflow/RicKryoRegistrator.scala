/*
 * Copyright (c) 2016 GAF AG, Munich, Germany. All rights reserved.
 */

package de.gaf.demo.workflow

import com.esotericsoftware.kryo.Kryo

import scala.reflect.ClassTag

/**
  * Registrator to register our custom classes with Kryo.
  *
  * Registers all classes used by GeoTrellis and some additional classes.
  */
class RicKryoRegistrator extends geotrellis.spark.io.kryo.KryoRegistrator {

  override def registerClasses(kryo: Kryo) {
    super.registerClasses(kryo)

    kryo.register(ClassTag(Class.forName("org.apache.spark.mllib.clustering.VectorWithNorm")).wrap.runtimeClass)
    kryo.register(Class.forName("org.apache.spark.mllib.clustering.VectorWithNorm"))
    kryo.register(classOf[org.apache.spark.mllib.linalg.DenseVector])
    kryo.register(ClassTag(Class.forName("scala.collection.mutable.ArrayBuffer")).wrap.runtimeClass)
    kryo.register(ClassTag(Class.forName("org.apache.spark.mllib.clustering.VectorWithNorm")).wrap.wrap.runtimeClass)
    kryo.register(Class.forName("geotrellis.proj4.CRS$$anon$3"))
    kryo.register(Class.forName("geotrellis.proj4.CRS$$anon$1"))
    kryo.register(classOf[org.osgeo.proj4j.proj.StereographicAzimuthalProjection])
    kryo.register(classOf[Array[org.apache.spark.mllib.linalg.Vector]])
    kryo.register(classOf[org.osgeo.proj4j.proj.LambertAzimuthalEqualAreaProjection])
    kryo.register(classOf[org.apache.spark.mllib.regression.LabeledPoint])
    kryo.register(classOf[Array[org.apache.spark.mllib.regression.LabeledPoint]])
    kryo.register(classOf[org.apache.spark.mllib.tree.model.Split])
    kryo.register(classOf[Array[org.apache.spark.mllib.tree.model.Split]])
    kryo.register(Class.forName("scala.Enumeration$Val"))
    kryo.register(Class.forName("org.apache.spark.mllib.tree.configuration.FeatureType$"))
    kryo.register(Class.forName("org.apache.spark.mllib.tree.model.DummyLowSplit"))
    kryo.register(Class.forName("org.apache.spark.mllib.tree.model.DummyHighSplit"))
    kryo.register(Class.forName("org.apache.spark.mllib.tree.impurity.GiniAggregator"))
    kryo.register(Class.forName("scala.collection.immutable.Map$EmptyMap$"))
    kryo.register(Class.forName("org.apache.spark.mllib.tree.impurity.Gini$"))
    kryo.register(Class.forName("org.apache.spark.mllib.tree.configuration.QuantileStrategy$"))
    kryo.register(Class.forName("scala.collection.immutable.Set$EmptySet$"))
    kryo.register(classOf[org.apache.spark.mllib.tree.model.InformationGainStats])
    kryo.register(classOf[org.apache.spark.mllib.tree.model.Predict])
    kryo.register(classOf[org.apache.spark.mllib.tree.model.DecisionTreeModel])
    kryo.register(Class.forName("org.apache.spark.mllib.tree.configuration.Algo$"))
    kryo.register(classOf[org.apache.spark.mllib.tree.model.Node])
    kryo.register(classOf[org.apache.spark.sql.types.StructType])
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructType]])
    kryo.register(classOf[org.apache.spark.sql.types.StructField])
    kryo.register(classOf[Array[org.apache.spark.sql.types.StructField]])
    kryo.register(Class.forName("org.apache.spark.sql.types.IntegerType$"))
    kryo.register(classOf[org.apache.spark.sql.types.Metadata])
    kryo.register(Class.forName("org.apache.spark.sql.types.DoubleType$"))
    kryo.register(Class.forName("org.apache.spark.sql.types.BooleanType$"))
    kryo.register(classOf[org.apache.spark.sql.types.ShortType])
    kryo.register(classOf[org.apache.spark.sql.types.ArrayType])
    kryo.register(Class.forName("org.apache.spark.mllib.tree.model.DecisionTreeModel$SaveLoadV1_0$NodeData"))
    kryo.register(Class.forName("org.apache.spark.mllib.tree.model.DecisionTreeModel$SaveLoadV1_0$PredictData"))
    kryo.register(Class.forName("org.apache.spark.mllib.tree.model.DecisionTreeModel$SaveLoadV1_0$SplitData"))
    kryo.register(classOf[Array[Object]])
    kryo.register(ClassTag(Class.forName("org.apache.spark.mllib.tree.model.DecisionTreeModel$SaveLoadV1_0$NodeData")).wrap.runtimeClass)
    kryo.register(classOf[Array[geotrellis.raster.MultibandTile]])
    kryo.register(classOf[Array[geotrellis.vector.ProjectedExtent]])
    kryo.register(Class.forName("org.apache.spark.ml.tree.impl.BaggedPoint"))
    kryo.register(Class.forName("org.apache.spark.ml.tree.impl.TreePoint"))
    kryo.register(classOf[com.typesafe.scalalogging.Logger])
    kryo.register(classOf[Array[org.apache.spark.ml.tree.Split]])
    kryo.register(classOf[org.apache.spark.ml.tree.ContinuousSplit])
    kryo.register(Class.forName("org.apache.spark.ml.tree.impl.DTStatsAggregator"))
    kryo.register(Class.forName("org.apache.spark.ml.tree.impl.DecisionTreeMetadata"))
    kryo.register(classOf[Array[java.lang.Double]])
    kryo.register(Class.forName("org.apache.spark.mllib.tree.model.ImpurityStats"))
    kryo.register(Class.forName("org.apache.spark.mllib.tree.impurity.GiniCalculator"))
    kryo.register(classOf[java.time.ZonedDateTime])
    kryo.register(classOf[java.time.LocalDateTime])
    kryo.register(classOf[java.time.LocalDate])
    kryo.register(classOf[java.time.LocalTime])
    kryo.register(classOf[java.time.ZoneOffset])
    kryo.register(classOf[org.slf4j.impl.Log4jLoggerAdapter])
  }
}
