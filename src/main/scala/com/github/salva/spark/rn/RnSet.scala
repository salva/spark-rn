package com.github.salva.spark.rn

import com.github.salva.spark.rn.impl.KDTree
import com.github.salva.spark.rn.metric.Euclidean
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.mllib.linalg.Vector

trait RnSet extends Serializable {
  def autoJoinInBall(distance:Double):Dataset[(RnSet.PointId, RnSet.PointId, Double)]
}

object RnSet {

  type PointId = Long

  def apply(ds:Dataset[(PointId,Vector)], metric:Metric):RnSet =
    new KDTree(ds, metric)
  def apply(ds:Dataset[(PointId,Vector)]):RnSet = apply(ds, Euclidean)

  def apply(df:DataFrame, id:String, rn: String, metric:Metric):RnSet = {
    import df.sqlContext.implicits._
    val ds = df.select(id, rn).map(row => (row.getLong(0), row.getAs[Vector](1)))
    new KDTree(ds, metric)
  }
  def apply(df:DataFrame, id:String, rn:String):RnSet = apply(df, id, rn, Euclidean)

  def autoJoinInBall(ds:Dataset[(PointId, Vector)], ballRadius:Double):Dataset[(PointId, PointId, Double)] = {
    val tree = RnSet(ds)
    tree.autoJoinInBall(ballRadius)
  }
}
