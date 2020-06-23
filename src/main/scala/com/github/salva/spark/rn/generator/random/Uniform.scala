package com.github.salva.spark.rn.generator.random

import com.github.salva.spark.rn.{Box,Rn}
import com.github.salva.spark.rn.RnSet.PointId
import org.apache.spark.sql.{Dataset, SparkSession}

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.random.RandomRDDs

object Uniform {
  private def mapToBox(v:org.apache.spark.mllib.linalg.Vector, box:Box):Vector = {
    val av = box.a.toArray
    val bv = box.b.toArray
    val out = new Array[Double](av.length)
    v.foreachActive {
      (i, x) => out(i) = av(i) * x + bv(i) * (1.0-x)
    }
    Rn(out)
  }

  def generateDataset(spark: SparkSession, n:Long, box:Box): Dataset[(PointId, Vector)] = {
    val dim = box.a.size
    import spark.implicits._
    val rdd =
      RandomRDDs
        .uniformVectorRDD(spark.sparkContext, n, dim)
        .zipWithIndex
        .map { case (v, id) => (id.asInstanceOf[PointId], mapToBox(v, box)) }
    spark.createDataset(rdd)
  }
}
