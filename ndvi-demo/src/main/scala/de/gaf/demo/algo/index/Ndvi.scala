/*
 * Copyright (c) 2016 GAF AG, Munich, Germany. All rights reserved.
 */

package de.gaf.demo.algo.index

import geotrellis.raster.{FloatConstantNoDataCellType, MultibandTile, Tile, _}

/**
  * Vegetation index NDVI from the red and infrared bands of a multiband raster.
  */
object Ndvi extends SpectralIndex {

  /**
    * Returns a Tile with NDVI values given that in the input multiband tile, the red band has the
    * index 0 and the infrared band has the index 1.
    *
    * If you have to select the bands manually, use withBands.
    *
    * @param multibandTile Input multiband tile of red band (index 0) and infrared band (index 1).
    * @return Tile with NDVI values.
    */
  override def apply(multibandTile: MultibandTile): Tile =
    multibandTile
      .convert(FloatConstantNoDataCellType)
      .combineDouble(0, 1) { ndviFunction }

  /**
    * Returns an Ndvi function with custom bands of the original multiband tile.
    *
    * @param redBand Index of the red band.
    * @param infraredband Index of the infrared band.
    * @return Function that applies the Ndvi to the selected bands.
    */
  def withBands(redBand: Int, infraredband: Int): SpectralIndex =
    (multibandTile: MultibandTile) => apply(multibandTile.subsetBands(redBand, infraredband))

  // NDVI base function.
  private def ndviFunction(red: Double, infrared: Double): Double =
    if(isData(red) && isData(infrared)) { (infrared - red) / (infrared + red) }
    else Float.NaN
}
