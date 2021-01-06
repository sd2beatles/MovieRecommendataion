package org.codecraftlabs
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType,LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}



case class Movies(userID:Int,movieID:Int,rating:Int)
case class Pairs(movie1:Int,movie2:Int,rating1:Double,rating2:Double)
object setMovies extends BaseSetHandler[Movies]{
  override val fileName:String="/u.data"
  override val cols:List[String]=List("userID","movieID","rating","timeStamp")
  override def getSchema(colName:List[String]):StructType={
    val schema=new StructType()
      .add(colName(0),IntegerType,nullable=true)
      .add(colName(1),IntegerType,nullable=true)
      .add(colName(2),IntegerType,nullable=true)
      .add(colName(3),LongType,nullable=true)
    schema
  }
  override def readContent(spark:SparkSession)(implicit enc:Encoder[Movies]):Dataset[Movies]= {

    val df=spark.read.option("sep","\t").schema(getSchema(cols)).csv(fileName)
    import spark.implicits._
    val movies=df.select("userID","movieID","rating").as[Movies]
    movies
  }

  def makePairs(spark:SparkSession):Dataset[Pairs]={
    import spark.implicits._
    val df=readContent(spark)
    val pairs=df.as("tb1")
      .join(df.as("tb2"),col("tb1.userID")===col("tb2.userID")&&col("tb1.movieID")<col("tb2.movieID"))
      .select(col("tb1.movieID").as("movie1"),col("tb2.movieID").as("movie2"),
        col("tb1.rating").as("rating1"),col("tb2.rating").as("rating2")).as[Pairs]
    pairs
  }
}
