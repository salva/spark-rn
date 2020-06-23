package com.github.salva.spark.rn.metric

import java.lang.Math.sqrt

import org.apache.spark.mllib.linalg.Vector

import com.github.salva.spark.rn.{Box, Metric, Rn}

object Euclidean extends Metric {

  private def sqr(a:Double):Double = a*a

  override def start(d:Double):Double = d * d

  override def end(d:Double):Double = sqrt(d)

  override def norm(v:Vector):Double = {
    var acu = 0.0
    v.foreachActive { (_, x) => acu += sqr(x) }
    acu
  }

  override def distance(u:Vector, v:Vector):Double = {
    var acu = 0.0
    Rn.foreach2(u, v) { (_, xa, xb) => acu += sqr(xb - xa) }
    acu
  }

  override def distance(v:Vector, box:Box):Double = {
    var acu = 0.0
    Rn.foreach(v) {
      (i, x) => {
        val xa = box.a(i)
        val xb = box.b(i)
        val d = {
          if (x < xa) xb - x
          else if (x > xb) x - xb
          else 0
        }
        acu += sqr(d)
      }
    }
    acu
  }

  override def distance(box0:Box, box1:Box):Double = {
    val a1 = box1.a
    val b1 = box1.b
    var acu = 0.0
    Rn.foreach2(box0.a, box0.b) {
      (i, xa0, xb0) =>
        val xa1 = a1(i)
        val xb1 = b1(i)
        if (xb0 <= xa1) acu += sqr(xb0 - xa1)
        else if (xb1 <= xa0) acu += sqr(xb1 - xa0)
    }
    acu
  }
}
