package org.codecraftlabs
import org.apache.spark.sql.{Dataset,SparkSession,Encoder}


//For a detailed explanation on parameterizing encoder, refer to the "Code Explanation" folder

trait BaseSetHandler[T]{
  val fileName:String
  val cols:List[String]
  def getSchema(colName:List[String]):StructType
  def readContent(spark:SparkSession)(implicit enc:Encoder[T]):Dataset[T]

}
