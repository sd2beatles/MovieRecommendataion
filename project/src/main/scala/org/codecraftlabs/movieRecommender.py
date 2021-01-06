package org.codecraftlabs
import org.apache.spark._
import org.apache.log4j._
import org.apache.log4j.Logger

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType,LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

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
