package com.github.salva.spark.rn.example

import com.github.salva.spark.rn.generator.random.Uniform
import com.github.salva.spark.rn.{Box, RnSet}
import org.apache.spark.sql.SparkSession

object TreeBuilder {
  def main(args: Array[String]): RnSet = {
    val n = if (args.length > 0) args(0).toInt else 20
    val dim = if (args.length > 1) args(1).toInt else 2
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    buildTree(spark, n, dim)
  }

  def buildTree(spark: SparkSession, n:Int, dim:Int):RnSet = {
    val points = Uniform.generateDataset(spark, n, Box.cube(dim))
    RnSet(points)
  }
}
