package com.knoldus.spark.graphx

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ Edge, Graph, VertexId, VertexRDD }
import org.apache.spark.rdd.RDD

class PropertyGraph(sparkContext: SparkContext) {
  def getGraph(vertices: RDD[(VertexId, (String, String))], edges: RDD[Edge[String]], defaultVertexAttr: (String, String)): Graph[(String, String), String] =
    Graph(vertices, edges, defaultVertexAttr)

  def getTripletView(graph: Graph[(String, String), String]): RDD[String] =
    graph.triplets.map(triplet => s"${triplet.srcAttr} is the ${triplet.attr} of ${triplet.dstAttr}")

  def getInDegree(graph: Graph[(String, String), String]): VertexRDD[Int] = graph.inDegrees
}
