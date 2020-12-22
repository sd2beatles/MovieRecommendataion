package org.codecraftlabs
import org.apache.spark.sql.{Dataset,SparkSession}


trait BaseSetHandler[T]{
  val cols:List[String]
  def getSchema(cols:List[String]):StructType
  def readContent(spark:SparkSession):Dataset[T]
}

