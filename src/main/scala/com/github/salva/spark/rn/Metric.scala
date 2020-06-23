package com.github.salva.spark.rn

import org.apache.spark.mllib.linalg.Vector

trait Metric extends Serializable {
  def start(v:Double):Double
  def end(d:Double):Double
  def norm(v:Vector):Double
  def distance(u:Vector, v:Vector):Double
  def distance(v:Vector, box:Box):Double
  def distance(boxA:Box, boxB:Box):Double
  def boxDiameter(box:Box) = distance(box.a, box.b)
}
