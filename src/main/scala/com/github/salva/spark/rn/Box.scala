package com.github.salva.spark.rn

import java.lang.Math.abs

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.{Encoder, Encoders, TypedColumn}

case class Box(val a:Vector, val b:Vector) extends Serializable {

  def copy = new Box(a.copy, b.copy)

  def +(other:Box) = {
    val av:Array[Double] = this.a.toArray.clone
    val bv:Array[Double] = this.b.toArray.clone
    Rn.foreach2(other.a, other.b) { (i, oax, obx) =>
      if (oax < av(i)) av(i) = oax
      if (obx > bv(i)) bv(i) = obx
    }
    Box(Rn(av), Rn(bv))
  }

  def +(rn:Vector):Box = {
    val av = this.a.toArray.clone
    val bv = this.b.toArray.clone
    Rn.foreach(rn) {
      (i, x) => {
        if (x < av(i)) av(i) = x
        else if (x > bv(i)) bv(i) = x
      }
    }
    Box(Rn(av), Rn(bv))
  }

  def longestDim: Int = {
    var maxI = 0
    var maxD = 0.0
    Rn.foreach2(a, b) {
      (i, xa, xb) => {
        val d = abs(xb - xa)
        if (d > maxD) {
          maxD = d
          maxI = i
        }
      }
    }
    maxI
  }

  def divideByLongestDim:(Box, Box) = {
    val i = longestDim
    val b0v = b.toArray.clone
    val a1v = a.toArray.clone
    val x = 0.5 * (a(i) + b(i))
    b0v(i) = x
    a1v(i) = x
    (Box(a, Rn(b0v)), Box(Rn(a1v), b))
  }

  def contains(rn: Vector):Boolean = {
    val av = a.toArray
    val bv = b.toArray
    val rnv = rn.toArray
    Iterator.range(0, rnv.length).forall {
      i => {
        val x = rnv(i)
        (x >= av(i)) && (x <= bv(i))
      }
    }
  }

  def volume = {
    var v = 1.0
    Rn.foreach2(a, b) { (i, xa, xb) => v *= (xb - xa) }
    v
  }

  def ab = {
    val v = b.toArray.clone
    Rn.foreach(a) { (i, ax) => v(i) -= ax }
    Rn(v)
  }

  override def toString = s"Box(${a}-${b})"
}

object Box {
  def fromRn(rn:Vector) = Box(rn, rn)

  def cube(dim:Int, xa:Double=0.0, xb:Double=1.0):Box = {
    if (xa < xb) Box(Rn.fill(dim)(xa), Rn.fill(dim)(xb))
    else Box(Rn.fill(dim)(xb), Rn.fill(dim)(xa))
  }

  def universe(dim:Int):Box = cube(dim, Double.MinValue, Double.MaxValue)

  def enclosingRns(rns:Iterator[Vector]):Box = {
    val av = rns.next.toArray.clone
    val bv = av.clone
    rns.foreach {
      vv => Rn.foreach(vv) {
        (i, x) => {
          if (x < av(i)) av(i) = x
          else if (x > bv(i)) bv(i) = x
        }
      }
    }
    Box(Rn(av), Rn(bv))
  }

  def foreach(box:Box)(f:(Int, Double, Double) => Unit):Unit = Rn.foreach2(box.a, box.b)(f)

  def aggregator[T](extract:T => Vector): TypedColumn[T,Box] = new BoxAggregator(extract).toColumn

}

private class BoxAggregator[T](val extract:T => Vector) extends org.apache.spark.sql.expressions.Aggregator[T, Option[Box], Box] {

  override def zero:Option[Box] = None
  override def reduce(oBox:Option[Box], t:T):Option[Box] = {
    val rn = extract(t)
    oBox match {
      case Some(box) => Some(box + rn)
      case None => Some(Box(rn, rn))
    }
  }

  override def merge(optBoxA:Option[Box], optBoxB:Option[Box]):Option[Box] = {
    (optBoxA, optBoxB) match {
      case (None, _) => optBoxB
      case (_, None) => optBoxA
      case (Some(boxA), Some(boxB)) => Some(boxA + boxB)
    }
  }

  override def finish(oBox:Option[Box]):Box = oBox.get

  def bufferEncoder: Encoder[Option[Box]] = Encoders.product[Option[Box]]

  def outputEncoder: Encoder[Box] = Encoders.product[Box]
}