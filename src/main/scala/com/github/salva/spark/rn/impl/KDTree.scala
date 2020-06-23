package com.github.salva.spark.rn.impl

import com.github.salva.spark.rn.RnSet.PointId
import com.github.salva.spark.rn.impl.KDTree.NodeId
import com.github.salva.spark.rn.{Box, Metric, RnSet}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.scalalang.typed.count
import org.apache.spark.sql.functions.lit

import scala.annotation.tailrec

object KDTree {
  type NodeId = Long
  val ROOT_INDEX:NodeId = 1
  val MAX_LEAF_SIZE:Int = 20

  val OPTIMAL_NODES_PER_PARTITION:Int = 1000


  @tailrec
  def buildTree(nodePoints: Dataset[NodePoint],
                prevNodes:Dataset[Node],
                prevDeadNodePoints:Dataset[NodePoint],
                metric:Metric,
                level:Int): (Dataset[Node], Dataset[NodePoint]) = {
    import nodePoints.sqlContext.implicits._

    Util.logger.info(s"Entering KDTree.buildTree, level: $level")

    // println(s"partitions: nodePoints->${nodePoints.rdd.getNumPartitions}, prevNodes->${prevNodes.rdd.getNumPartitions}, prevDeadNodePoints->${prevDeadNodePoints.rdd.getNumPartitions}")

    val nodes =
      nodePoints
        .groupByKey(_.nodeId)
        .agg(Box.aggregator(_.rn), count[NodePoint](_.pointId))
        .map {
          case (nodeId, box, count) =>
            Node(nodeId, nodeId >> 1, box, count, (metric.boxDiameter(box)) == 0.0 || (count <= MAX_LEAF_SIZE))
        }
        .checkpoint

    val liveNodes = nodes.filter(!_.leaf)

    // println(deadNodes.collect.toList.mkString("deadNodes:\n[ - ", "\n  - ", " ]\n"))
    // println(liveNodes.filter(!_.leaf).collect.toList.mkString("liveNodes:\n[ - ", "\n  - ", " ]\n"))

    val leftBoxes = liveNodes.map(n => (n.nodeId, n.box.divideByLongestDim._1))
    val nextNodePoints =
      nodePoints
        .joinWith(leftBoxes, nodePoints("nodeId") === leftBoxes("_1"))
        .map { case (np, (parentNodeId, leftBox)) =>
          val inBox = leftBox.contains(np.rn)
          val nodeId = (parentNodeId << 1) + (if (inBox) 0 else 1)
          NodePoint(nodeId, np.pointId, np.rn)
        }
        .checkpoint

    // println(nextNodePoints.take(250).toList.mkString(s"nextNodePoints ${nextNodePoints.count}:\n[ - ", "\n  - ", " ]\n"))

    val nodesSoFar = prevNodes.unionAll(nodes)

    val deadNodes = nodes.filter(_.leaf)
    val deadNodePoints = nodePoints.joinWith(deadNodes, nodePoints("nodeId") === deadNodes("nodeId")).map(_._1).checkpoint(true)
    val deadNodePointsSoFar = prevDeadNodePoints.unionAll(deadNodePoints)

    if (nextNodePoints.isEmpty) {
      val allNodes = nodesSoFar.cache
      val allNodesPartitions:Int = (allNodes.count().toInt + OPTIMAL_NODES_PER_PARTITION) / OPTIMAL_NODES_PER_PARTITION

      val allNodePointsPartitions = nodePoints.rdd.getNumPartitions

      (allNodes.coalesce(allNodesPartitions).checkpoint(true),
        deadNodePointsSoFar.coalesce(allNodePointsPartitions).checkpoint(true))
    }
    else buildTree(nextNodePoints, nodesSoFar, deadNodePointsSoFar, metric, level + 1)
  }
}

case class NodePoint(nodeId: KDTree.NodeId, pointId:PointId, rn:Vector) {
  override def toString = s"NodePoint(nodeId: $nodeId, pointId: $pointId, rn:$rn)"
}

case class Node(nodeId:NodeId, parentNodeId:NodeId, box:Box, count:Long, leaf:Boolean) {
  override def toString = s"Node(nodeId: $nodeId, parentNodeId: $parentNodeId, box: $box, count: $count, leaf:$leaf)"
}

class KDTree(val ds:Dataset[(PointId, Vector)], val metric:Metric) extends RnSet {

  val (nodes, points) = {
    import ds.sqlContext.implicits._
    KDTree.buildTree(ds.map(p => NodePoint(KDTree.ROOT_INDEX, p._1, p._2)),
      Seq[Node]().toDS, Seq[NodePoint]().toDS, metric, 0)
  }

  def dump(dumpPoints:Boolean=false) = {
    println(nodes.orderBy("nodeId").collect.toList.mkString("Tree nodes:\n[ - ", "\n  - ", " ]\n\n"))
    if (dumpPoints)
      println(points.orderBy("nodeId", "pointId").collect.toList.mkString("Points:\n[ - ", "\n  - ", " ]\n\n"))
  }

  case class NodePair(a:NodeId, b:NodeId, distance:Double)

  override def autoJoinInBall(distance:Double):Dataset[(PointId, PointId, Double)] = {
    import nodes.sqlContext.implicits._

    val metricDistance = metric.start(distance)

    val root = nodes.where(nodes("nodeId") === lit(KDTree.ROOT_INDEX)).first()
    val nodePairs =
      autoJoinInBallStep(Seq((root, root)).toDS,
        Seq[(NodeId, NodeId)]().toDS,
        metricDistance, 0)

    Util.logger.info("In KDTree.autoJoinInBall, merger nodes and points")

    val nodePairs1 = nodePairs.joinWith(points, nodePairs("_1") === points("nodeId"))
    nodePairs1
      .joinWith(points, nodePairs1("_1._2") === points("nodeId"))
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
