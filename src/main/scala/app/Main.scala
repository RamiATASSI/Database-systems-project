package app

import app.loaders.RatingsLoader
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)


    val ratingsLoader = new RatingsLoader(sc, "/ratings_medium_delta.csv")
    val ratings = ratingsLoader.load()
    //   for each user-title pair, count the number of ratings
    val ratingsCount = ratings.map(rating => ((rating._1, rating._2), 1)).reduceByKey(_ + _).filter(_._2 > 1)
    ratingsCount.foreach(println)
  }
}
