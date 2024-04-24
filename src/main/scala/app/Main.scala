package app

import app.*
import app.loaders.{MoviesLoader, RatingsLoader}
import org.apache.spark.{SparkConf, SparkContext}

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("app").setMaster("local[*]")
    val sc = SparkContext.getOrCreate(conf)


    val ratingsLoader = new MoviesLoader(sc, "/movies_small.csv")
    val ratings = ratingsLoader.load()
    ratings.foreach(println)
  }
}
