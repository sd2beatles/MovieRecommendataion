package org.codecraftlabs
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType,LongType}
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}



object setSimilarity{
  case class MovieSimilarity(movie1:Int,movie2:Int,score:Double,numPairs:Long)
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
