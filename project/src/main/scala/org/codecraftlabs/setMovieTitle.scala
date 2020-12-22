package org.codecraftlabs
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}



case class MovieTitle(movieID:Int,movieTitle:String)
object setMovieTitle extends BaseSetHandler[MovieTitle]{
  override val fileName:String="u.item"
  override val cols:List[String]=List("movieID","movieTitle")
  override def getSchema(colName:List[String]):StructType={
    val schema=new StructType()
      .add(colName(0),IntegerType,nullable=true)
      .add(colName(1),StringType,nullable=true)
    schema
  }
  override def readContent(spark:SparkSession)(implicit enc:Encoder[MovieTitle]):Dataset[MovieTitle]={
    import spark.implicits._
    spark.read.option("sep","|").option("charset","ISO-8859-1").schema(getSchema(cols)).csv(fileName).as[MovieTitle]
  }
}
