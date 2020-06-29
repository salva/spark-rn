package com.github.salva.spark.rn.impl.gridtree

import java.lang.Math.pow

import com.github.salva.spark.rn.RnSet.PointId
import com.github.salva.spark.rn.impl.common.Util
import com.github.salva.spark.rn.impl.gridtree.GridTree.NodeId
import com.github.salva.spark.rn.impl.tree.Tree
import com.github.salva.spark.rn.{Box, Metric}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.scalalang.typed.count
import org.apache.spark.sql.functions.lit

case class Node(nodeId:GridTree.NodeId, parentNodeId:GridTree.NodeId, box:Box,
                divisions:Option[Array[Int]], count:Long)
  extends com.github.salva.spark.rn.impl.tree.Node[GridTree.NodeId]
{

  override def toString = s"Node($nodeId, $parentNodeId, $box, ${divisionsToString}, $count)"

  private def divisionsToString = {
    divisions match {
      case Some(array) => array.mkString("[", ",", "]")
      case _ => "leaf"
    }
  }

  def leaf = divisions.isEmpty

}

case class NodePoint(nodeId:GridTree.NodeId, pointId:PointId, rn:Vector)
  extends com.github.salva.spark.rn.impl.tree.NodePoint[GridTree.NodeId]

object GridTree {
  val ROOT_INDEX = "1"
  val MAX_PARTITIONS = 100
  val MAX_LEAF_SIZE = 200

  type NodeId = String

  def generateTreeStep(nodePoints:Dataset[NodePoint], metric:Metric): (Dataset[Node], Dataset[NodePoint]) = {
    import nodePoints.sqlContext.implicits._

    Util.logger.warn(s"Entering GridTree.generateTreeStep, nodePoints: ${nodePoints.count}")

    val nodes =
      nodePoints
        .groupByKey(_.nodeId)
        .agg(Box.aggregator(_.rn), count[NodePoint](_.pointId))
        .map {
          case (nodeId, box, count) => {
            val divisions =
              if ((count > GridTree.MAX_LEAF_SIZE) && (metric.boxDiameter(box) > 0))
                Some(calculateDivisions(box.ab))
              else None

            val nodeIdBI = BigInt(nodeId)
            val parentNodeId = (nodeIdBI / GridTree.MAX_PARTITIONS).toString
            Node(nodeId, parentNodeId, box, divisions, count)
          }
        }
        .cache

    Util.logger.warn(s"Nodes generated: ${nodes.count}")

    if (nodes.count < 10) {
      println(nodes.collect.toList.mkString("nodes:\n[ ", "\n  ", " ]\n"))

    }

    if (nodes.filter(_.divisions.nonEmpty).isEmpty) {
      (nodes, nodePoints)
    }
    else {
      val nodeNodePoints = nodes.joinWith(nodePoints, nodes("nodeId") === nodePoints("nodeId")).checkpoint

      Util.logger.warn(s"Node-NodePoint pairs generated: ${nodeNodePoints.count}")

      // println(nodeNodePoints.take(100).mkString("Node-NodePoints:\n[ ", "\n  ", " ]\n"))

      val deadNodePoints = nodeNodePoints.flatMap(p => if (p._1.divisions.isEmpty) Some(p._2) else None)

      val liveNodePoints =
        nodeNodePoints
          .flatMap {
            case (Node(nodeId, _, box, Some(divisions), _), nodePoint) => {
              val parentNodeIdBI = BigInt(nodeId)
              val nodeIdStr = (parentNodeIdBI * MAX_PARTITIONS + partitionIx(nodePoint.rn, box, divisions)).toString
              Some(NodePoint(nodeIdStr, nodePoint.pointId, nodePoint.rn))
            }
            case _ => None
          }

      val (nextNodes, nextNodePoints) = generateTreeStep(liveNodePoints, metric)
      (nodes.unionAll(nextNodes), deadNodePoints.unionAll(nextNodePoints))
    }
  }

  def partitionIx(v:Vector, box:Box, divisions:Array[Int]): Int = {
    var ix = 0
    Box.foreach(box) {
      (i, ax, bx) => {
        val n = divisions(i)
        if (n > 1) {
          val abx = bx - ax
          val avx = v(i) - ax
          val c = ((n * avx) / abx).toInt
          ix = ix * n + (if (c < n) c else n - 1) // because of points on the box border
        }
      }
    }
    ix
  }

  def calculateDivisions(ab: Vector) = {
    val abv = ab.toArray
    val dim = abv.length
    val divisions = new Array[Int](dim)
    val sorted = abv.zipWithIndex.sortBy(_._1)
    var partitionsSoFar = 1
    sorted.zipWithIndex.foreach {
      case ((x, i), j) => {
        if (x==0) divisions(i) = 1
        else {
          val vol = Iterator.range(j, dim).map(sorted(_)._1).reduce(_*_)
          // println(s"$vol, ${GridTree.MAX_PARTITIONS}, ${partitionsSoFar}, $dim, $i")
          val u = pow(vol/(GridTree.MAX_PARTITIONS/partitionsSoFar), 1.0/(dim - j))
          if (u >= x) divisions(i) = 1
          else {
            val d = (x / u).toInt
            divisions(i) = d
            partitionsSoFar *= d
          }
        }
      }
    }
    // println(s"$ab divisions => [${divisions.mkString(",")}]")
    divisions
  }
}

class GridTree(val ds:Dataset[(PointId, Vector)], val metric:Metric)
  extends Tree[GridTree.NodeId, Node, NodePoint] {

  def rootNodeId = GridTree.ROOT_INDEX

  val (nodes, nodePoints) = {
    import ds.sqlContext.implicits._
    val nodePoints = ds.map(p => NodePoint(GridTree.ROOT_INDEX.toString, p._1, p._2))
    GridTree.generateTreeStep(nodePoints, metric)
  }


  override def autoJoinInBall(distance:Double):Dataset[(PointId, PointId, Double)] = {
    import nodes.sqlContext.implicits._

    val metricDistance = metric.start(distance)

    val root = nodes.where(nodes("nodeId") === lit(rootNodeId)).first()
    val nodePairs =
      autoJoinInBallStep(Seq((root, root)).toDS,
        (Seq():Seq[(NodeId, NodeId)]).toDS,
        metricDistance, 0)

    Util.logger.info("In GridTree.autoJoinInBall, nodes and points merged")

    val nodePairs1 = nodePairs.joinWith(nodePoints, nodePairs("_1") === nodePoints("nodeId"))
    nodePairs1
      .joinWith(nodePoints, nodePairs1("_1._2") === nodePoints("nodeId"))
      .map { case (((_, _), pointA), pointB) => (pointA.pointId, pointB.pointId, metric.distance(pointA.rn, pointB.rn)) }
      .filter(_._3 <= metricDistance)
      .distinct
      .map(p => (p._1, p._2, metric.end(p._3)))
  }

  def autoJoinInBallStep(start:Dataset[(Node, Node)],
                         prevLeaf:Dataset[(NodeId, NodeId)],
                         metricDistance:Double,
                         level:Int):Dataset[(NodeId, NodeId)] = {

    Util.logger.info(s"entering KDTree.autoJoinInBallStep, level: $level")

    import nodes.sqlContext.implicits._

    val leafA = start.filter((start("_1.leaf") === lit(true)) && (start("_1.leaf") === lit(false)))
    val leafB = start.filter((start("_1.leaf") === lit(false)) && (start("_1.leaf") === lit(true)))
      .map { case (nodeA, nodeB) => (nodeB, nodeA) }
    val leafOne = leafA.unionAll(leafB)
    val nextLeafOne =
      leafOne
        .joinWith(nodes, leafOne("_2.nodeId") === nodes("nodeId"))
        .map { case ((nodeA, _), nodeB) => (nodeA, nodeB) }

    val noLeaf = start.filter((start("_1.leaf") === lit(false)) && (start("_2.leaf") === lit(false)))
    val nextNoLeaf1 = noLeaf.joinWith(nodes, noLeaf("_1.nodeId") === nodes("parentNodeId"))
    val nextNoLeaf = nextNoLeaf1.joinWith(nodes, nextNoLeaf1("_1._2.nodeId") === nodes("parentNodeId"))
      .map { case (((_, _), nodeA), nodeB) => (nodeA, nodeB) }

    val next =
      nextLeafOne
        .union(nextNoLeaf)
        .filter {
          (p:(Node,Node)) => (metric.distance(p._1.box, p._2.box) <= metricDistance)
        }
        .checkpoint

    // println(next.map(p => s"${p._1} <=> ${p._2}").collect.mkString("next:\n[ ", "\n  ", " ]\n"))

    val leafSoFar =
      start
        .filter((start("_1.leaf") === lit(true)) && (start("_1.leaf") === lit(true)))
        .map { case (nodeA, nodeB) => (nodeA.nodeId, nodeB.nodeId) }
        .checkpoint
        .unionAll(prevLeaf)

    if (next.isEmpty) leafSoFar.cache
    else autoJoinInBallStep(next, leafSoFar, metricDistance, level+1)
  }

}
