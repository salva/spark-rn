package com.github.salva.spark.rn.impl.tree

import com.github.salva.spark.rn.{Box, Metric, RnSet}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Dataset

abstract class Node[NodeId] extends Product {
  val nodeId:NodeId
  val parentNodeId:NodeId
  val box:Box
  def leaf:Boolean
}

abstract class NodePoint[NodeId] extends Product {
  val nodeId:NodeId
  val pointId: Long
  val rn:Vector
}

trait Tree[NodeId, Node1 <: Node[NodeId], NodePoint1 <: NodePoint[NodeId]] extends RnSet {

  val nodes:Dataset[Node1]
  val nodePoints:Dataset[NodePoint1]
  val metric:Metric

  def rootNodeId:NodeId

  override def dump(dumpPoints: Boolean):Unit = {
    println(nodes.orderBy("nodeId").take(20).toList.mkString("Tree nodes:\n[ - ", "\n  - ", " ]\n\n"))
    if (dumpPoints)
      println(nodePoints.orderBy("nodeId", "pointId").collect.toList.mkString("Points:\n[ - ", "\n  - ", " ]\n\n"))
  }


}
