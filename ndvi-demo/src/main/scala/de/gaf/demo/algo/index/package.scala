/*
 * Copyright (c) 2016 GAF AG, Munich, Germany. All rights reserved.
 */

package de.gaf.demo.algo

import geotrellis.raster.{MultibandTile, Tile}

package object index {
  /**
    * Spectral indices of satellite data map multiband spectral raster data to singleband raster data.
    */
  type SpectralIndex = MultibandTile => Tile
}
