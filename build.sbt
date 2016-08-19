import org.scalastyle.sbt.ScalastylePlugin.scalastyleConfig

name := "spark-graphx-cassandra"

version := "1.0"

scalaVersion := "2.11.8"

organization := "com.knoldus"

scalastyleConfig in Compile :=  file("Knoldus-spark-scalastyle-config-v0.1.xml")