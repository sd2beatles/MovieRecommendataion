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
import org.codecraftlabs.MovieRecommender


object main {
  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    @transient lazy val logger=Logger.getLogger(getClass.getName)
    logger.info("Processing Movie data information")
    val spark=SparkSession
      .builder
      .appName("test")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    logger.info("Read the data source and make a dataframe for movie pairs")
    val pairs=setMovies.makePairs(spark)
    logger.info("choose the distance metrics of the four.In our case,we take minkowski as the distance measure")
    val dataPairs=pairs.minkowski
    logger.info("Load the data with movieId and its name")
    val titles=setMovieTitle.readContent(spark)
    
    //print out  all the movie(s) closely related to the given input MovieId
    logger.info("create an instance of the class")
    val recommnder=new MovieRecommender(dataPairs,titles)
    logger.info("print out most related movies by the score")
    recommnder.printOut
  }
}
