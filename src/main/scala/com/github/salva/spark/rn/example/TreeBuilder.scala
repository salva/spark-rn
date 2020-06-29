package com.github.salva.spark.rn.example

import com.github.salva.spark.rn.RnSet.PointId
import com.github.salva.spark.rn.generator.random.Uniform
import com.github.salva.spark.rn.{Box, RnSet}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.mllib.linalg.Vector


object TreeBuilder {
  def main(args: Array[String]): Unit = {
    val n = if (args.length > 0) args(0).toInt else 20000000
    val dim = if (args.length > 1) args(1).toInt else 2
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    spark.sparkContext.setCheckpointDir("/tmp/spark-checkpoints")

    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.toLevel("WARN"))

    val rnSet = spark.time(buildTree(spark, n, dim))
    rnSet.dump(false)
  }

  def buildTree(spark: SparkSession, n:Int, dim:Int):RnSet = {
    val points = generateSampleData(spark, n, dim)
    RnSet(points)
  }

  def generateSampleData(spark: SparkSession, n:Int, dim:Int):Dataset[(PointId, Vector)] = {
    val data = Uniform.generateDataset(spark, n, Box.cube(dim))
    println("Data generated")
    data
  }
}
