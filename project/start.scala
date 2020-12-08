package org.codecraflabs
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType,LongType,StructType,StringType}
import org.apache.spark.sql.{DataFrame,Dataset}
import org.codecraflabs.setMovieNames._
import org.codecraflabs.setMovies._


trait BaseDataHandler[A]{
  val cols:List[String]
  def getSchema(colNames:List[String]):StructType
  def readContent(spark:SparkSession):Dataset[A]
}

case class MovieTitle(movieID:Int,movieTitle:String)
object setMovieNames extends BaseDataHandler[MovieTitle]{
  override val cols:List[String]=List("movieID","movieTitle")

  override def getSchema(colNames:List[String]):StructType={
    val schema=new StructType()
      .add(colNames(0),IntegerType,nullable=true)
      .add(colNames(1),StringType,nullable=true)
    schema
  }
  override def readContent(spark:SparkSession):Dataset[MovieTitle]={
  import spark.implicits._
    spark.read.option("sep","|").option("charset","ISO-8859-1").schema(getSchema(cols))
      .csv("C:/SparkScalaCourse/SparkScalaCourse/data/ml-100k/ml-100k/u.item")
      .as[MovieTitle]
  }
}
case class Movies(userID:Int,MovieID:Int,rating:Int,timeStamp:Long)
case class MovieMini(userID:Int,movieID:Int,rating:Int)
object setMovies extends BaseDataHandler[Movies]{
  override val cols:List[String]=List("userID","movieID","rating","timeStamp")

  override def getSchema(colNames:List[String]):StructType={
    val schema=new StructType()
      .add(colNames(0),IntegerType,nullable=true)
      .add(colNames(1),IntegerType,nullable=true)
      .add(colNames(2),IntegerType,nullable=true)
      .add(colNames(3),LongType,nullable=true)
    schema
  }
  override def readContent(spark:SparkSession):Dataset[Movies]={
  import spark.implicits._
    spark.read.option("sep","\t").schema(getSchema(cols)).csv("C:/SparkScalaCourse/SparkScalaCourse/data/ml-100k/ml-100k/u.data")
      .as[Movies]
  }

  def slicedData(spark:SparkSession,df:Dataset[Movies]):Dataset[MovieMini]={
    import spark.implicits._
    df.select("userID","movieID","rating").as[MovieMini]
  }
}

case class MoviePairs(movie1:Int,movie2:Int,rating1:Int,rating2:Int)
case class MovieSimilarity(movie1:Int,movie2:Int,score:Double,numPairs:Long)
object computeSimilarity{
  def makeMoviePairs(spark:SparkSession,df:Dataset[MovieMini]):Dataset[MoviePairs]= {
    import spark.implicits._
    df.as("rating1")
      .join(df.as("rating2"),col("rating1.userID")===col("rating2.userID") &&
        col("rating1.movieID")<col("rating2.movieID"))
      .select(col("rating1.movieID").as("movie1"),
        col("rating2.movieID").as("movie2"),
        col("rating1.rating").as("rating1"),
        col("rating2.rating").as("rating2")).as[MoviePairs]
  }

  def calculateSimilarity(spark:SparkSession,data:Dataset[MoviePairs]):Dataset[MovieSimilarity]={
    val pairScore=data.withColumn("xx",col("rating1")*col("rating1"))
      .withColumn("yy",col("rating2")*col("rating2"))
      .withColumn("xy",col("rating1")*col("rating2"))

    val similarityDS=pairScore.groupBy("movie1","movie2")
      .agg(sum(col("xy")).alias("numerator"),
        (sqrt(sum(col("xx")))*sqrt(sum(col("yy")))).alias("denominator"),
        count(col("xy")).alias("numPairs"))
    import spark.implicits._
    val result=similarityDS
      .withColumn("score"
        ,when(col("denominator")=!=0,col("numerator")/col("denominator"))
          .otherwise(null))
      .select("movie1","movie2","score","numPairs").as[MovieSimilarity]

    result
  }
}

  object presentData{
    def searchData(data:Dataset[MovieSimilarity],movieID:Int,socreThreshold:Double=0.5,conCurrentTrheshold:Long=50):Dataset[MovieSimilarity]={
      val filteredData=data.filter(col("movie1")===movieID ||col("movie2")===movieID &&
        col("score")>socreThreshold && col("numPairs")>conCurrentTrheshold)
    filteredData
    }

    def getMovieName(data:Dataset[MovieTitle],movieID:Int):String={
      val result=data.filter(col("movieID")===movieID).select("movieTitle").collect()(0)
      result(0).toString
    }

    def printSample(data:Dataset[MovieSimilarity],movieName:Dataset[MovieTitle],sampleNumber:Int,movieID:Int)={
      val results=data.sort(col("score").desc).take(sampleNumber)
      for(result<-results){
        var similarMovieID=result.movie1
        if(similarMovieID==movieID){
          similarMovieID=result.movie2
        }
        println(getMovieName(movieName,similarMovieID)+"\tscore:"+result.score+"\tstrength:"+result.numPairs)
      }
    }
  }



object main{
  def main(args:Array[String]) {
    @transient lazy val logger=Logger.getLogger(getClass.getName)
    Logger.getLogger("org").setLevel(Level.ERROR)
    logger.info("Begin processing data")

    val spark=SparkSession
      .builder
      .appName("movieRecommendation")
      .master("local[*]")
      .getOrCreate()

    logger.info("Loading MovieTitle")
    val movieNames=setMovieNames.readContent(spark)

    logger.info("Loading movie data and select the first three columns")
    val data=setMovies.readContent(spark)
    val dfSelected=setMovies.slicedData(spark,data)

    logger.info("make movie paris with duplicate entries filtered out")
    val moviePairs=computeSimilarity.makeMoviePairs(spark,dfSelected)

    logger.info("compute similarity scores as well as the counts of connections")
    val similarityScore=computeSimilarity.calculateSimilarity(spark,moviePairs)

    logger.info("filter the data")
    if(args.length>0){
      val movieID=args(0).toInt
      val filtered=presentData.searchData(similarityScore,movieID,0.6,conCurrentTrheshold = 60)
      presentData.printSample(filtered,movieNames,10,movieID)
    }


  }
}
