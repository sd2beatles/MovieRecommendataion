package org.codecraftlabs
import org.apache.spark._
import org.apache.log4j._
import org.apache.log4j.Logger

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType,LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import scala.collection.mutable.ArrayBuffer
import org.codecraftlabs.setSimilarity._
import org.codecraftlabs.setMovieTitle._
import org.codecraftlabs.setSimilarity._




//For a detailed explanation on parameterizing encoder, refer to the "Code Explanation" folder

trait BaseSetHandler[T]{
  val fileName:String
  val cols:List[String]
  def getSchema(colName:List[String]):StructType
  def readContent(spark:SparkSession)(implicit enc:Encoder[T]):Dataset[T]

}


case class MovieTitle(movieID:Int,movieNames:String)
object setMovieTitle extends BaseSetHandler[MovieTitle]{
  override val fileName:String="C:/SparkScalaCourse/SparkScalaCourse/data/ml-100k/u.item"
  override val cols:List[String]=List("movieID","movieNames")
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


case class Movies(userID:Int,movieID:Int,rating:Int)
case class Pairs(movie1:Int,movie2:Int,rating1:Double,rating2:Double)
object setMovies extends BaseSetHandler[Movies]{
  override val fileName:String="C:/SparkScalaCourse/SparkScalaCourse/data/ml-100k/u.data"
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


/*
 Create our own method called cosineSimilarity and put it inside an object. For more info,please refer to the section of code reivew.
*/


object setSimilarity{
  implicit class distanceMeasures(pairs:Dataset[Pairs]){
    def cosineSimilarity={
      val scores=pairs
        .withColumn("xx",col("rating1")*col("rating1"))
        .withColumn("yy",col("rating2")*col("rating2"))
        .withColumn("xy",col("rating1")*col("rating2"))

      val similarity=scores.groupBy("movie1","movie2")
        .agg(sum(col("xy")).alias("numerator"),
          (sqrt(sum(col("yy")))*sqrt(sum(col("xx")))).alias("denominator"),
          count(col("xy")).alias("numPairs"))

      val result=similarity
        .withColumn("score",when(col("numerator")=!=0,col("numerator")/col("denominator")).otherwise(null))
        .select("movie1","movie2","score","numPairs")
      result
    }

    def euclidean:DataFrame={
      val scores=pairs
        .withColumn("x-y",col("rating1")-col("rating2"))

      val similarity=scores.groupBy("movie1","movie2")
        .agg(sqrt(sum(col("x-y")*col("x-y"))).alias("score"),count("x-y").alias("numPairs"))
        .select("movie1","movie2","score","numPairs")
      similarity
    }

    def minkowski:DataFrame={
      val similarity=pairs
        .withColumn("absDiff",abs(col("rating1")-col("rating2")))
        .groupBy("movie1","movie2")
        .agg(sqrt(sum("absDiff")).alias("score"),count("absDiff").alias("numPairs"))
      similarity
    }
  }
}


class MovieRecommender(val data:DataFrame,val titles:Dataset[MovieTitle],var movieId:Int,conCurrentThread:Double,sampleNum:Int){
  def this(data:DataFrame,titles:Dataset[MovieTitle]){
    this(data,titles,movieId = 744,conCurrentThread=1,sampleNum=1)
  }


  def getName(movieId:Int):String={
    val name=titles.filter(col("movieID")===movieId).select("movieNames").collect()(0)
    name(0).toString
  }
  def printOut={
    val filterData=data.filter(col("movie1")===movieId||col("movie2")===movieId &&col("numPairs")>conCurrentThread)
    val results=filterData.sort(col("score").desc).take(sampleNum)
    for(result<-results){
      var similar=result(0).toString.toInt
      if(movieId==similar){
        similar=result(1).toString.toInt
      }
      println(getName(similar)+"\tscore: "+result(2)+"\tstrength: "+result(3))
    }
  }
}




object main {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark=SparkSession
      .builder
      .appName("test")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val pairs=setMovies.makePairs(spark).minkowski
    val titles=setMovieTitle.readContent(spark)
    val recommnder=new MovieRecommender(pairs,titles)
    recommnder.printOut
  }
}
