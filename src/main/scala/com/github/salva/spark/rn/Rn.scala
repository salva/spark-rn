package com.github.salva.spark.rn

import org.apache.spark.mllib.linalg.{DenseVector, Vector, Vectors}

object Rn {
  def foreach(v:Vector)(f:(Int, Double) => Unit):Unit = {
    Iterator.range(0, v.size).foreach(i => f(i, v(i)))
  }

  def foreach2(u:Vector, v:Vector)(f:(Int, Double, Double) => Unit):Unit = {
    Iterator.range(0, u.size).foreach(i => f(i, u(i), v(i)))
  }

  def apply(values: Array[Double]) = new DenseVector(values)

  def fill(dim:Int)(value:Double) = new DenseVector(Array.fill(dim)(value))

  def zeros(dim:Int) = Vectors.zeros(dim)

}
