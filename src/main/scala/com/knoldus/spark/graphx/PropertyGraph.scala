package com.knoldus.spark.graphx

import org.apache.spark.SparkContext

class VertexProperty()
case class UserProperty(val name: String) extends VertexProperty
case class ProductProperty(val name: String, val price: Double) extends VertexProperty

class PropertyGraph(sparkContext: SparkContext) {

}
