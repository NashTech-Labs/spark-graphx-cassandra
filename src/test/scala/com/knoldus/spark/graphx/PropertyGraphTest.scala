package com.knoldus.spark.graphx

import org.apache.spark.graphx.{ Edge, Graph, VertexId }
import org.apache.spark.rdd.RDD
import org.apache.spark.{ SparkConf, SparkContext }
import org.scalatest.FunSuite

object TestData {
  val sparkContext = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))

  val users: RDD[(VertexId, (String, String))] =
    sparkContext.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
  val relationships: RDD[Edge[String]] =
    sparkContext.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
  val defaultUser = ("John Doe", "Missing")
}

class PropertyGraphTest extends FunSuite {
  import com.knoldus.spark.graphx.TestData._

  val propertyGraph = new PropertyGraph(sparkContext)

  test("property graph returns graph") {
    val graph = propertyGraph.getGraph(users, relationships, defaultUser)
    assert(graph.edges.count() === 4)
  }

  test("property graph returns triplets in a graph") {
    val graph = propertyGraph.getTripletView(Graph(users, relationships, defaultUser))
    assert(graph.count() === 4)
  }

  test("property graph returns indegree of a graph") {
    val graph = propertyGraph.getInDegree(Graph(users, relationships, defaultUser))
    assert(graph.count() === 3)
  }

  test("property graph returns subgraph of a graph") {
    val users: RDD[(VertexId, (String, String))] =
      sparkContext.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")), (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    val relationships: RDD[Edge[String]] =
      sparkContext.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"), Edge(4L, 0L, "student"),
        Edge(5L, 0L, "colleague")))
    val defaultUser = ("John Doe", "Missing")

    val subGraph = propertyGraph.getSubGraph(Graph(users, relationships, defaultUser), { (id: Long, attr: (String, String)) => attr._2 != "Missing" })
    assert(subGraph.edges.count() === 4)
  }
}
