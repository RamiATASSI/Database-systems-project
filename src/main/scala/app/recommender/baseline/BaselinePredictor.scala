package app.recommender.baseline

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD


class BaselinePredictor() extends Serializable {
  private var avgByUserID: RDD[(Int, Double)] = _
  private var movieGlobalAvgDeviation: RDD[(Int, Double)] = _
  private var globalAVG: Double = _

  def init(ratingsRDD: RDD[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val partitioner = new HashPartitioner(ratingsRDD.partitions.length)
    globalAVG = ratingsRDD.map(_._4).sum() / ratingsRDD.count()

    val byUserID = ratingsRDD.groupBy(_._1)
    avgByUserID = byUserID.mapValues(_.map(_._4)).mapValues(x => x.sum / x.size)
      .partitionBy(partitioner).persist()

    val ratingsJoinedUserAVG = ratingsRDD.map(x => (x._1, (x._2, x._4))).join(avgByUserID)
    val adjustedRatings = ratingsJoinedUserAVG.map {
      case (userId, ((movieID, rating), avgRating)) =>
        ((userId, movieID), normalizedDeviation(rating, avgRating))
    }.partitionBy(partitioner)

    val byMovieID = adjustedRatings.groupBy(_._1._2)
    movieGlobalAvgDeviation = byMovieID.mapValues(_.map(_._2)).mapValues(x => x.sum / x.size)
      .partitionBy(partitioner).persist()
  }

  private def normalizedDeviation(rating: Double, avgRating: Double): Double = {
    (rating - avgRating) / scaleStandardization(rating, avgRating)
  }

  private def getAvgRatingForUser(userId: Int): Double = {
    val userAVG = avgByUserID.lookup(userId)
    if (userAVG.nonEmpty) userAVG.head else globalAVG
  }

  private def scaleStandardization(x: Double, userAVG: Double): Double = {
    if x > userAVG then 5 - userAVG
    else if x < userAVG then userAVG - 1
    else 1
  }

  def predict(userId: Int, movieId: Int): Double = {
    val userAVG = getAvgRatingForUser(userId)
    val movieGlobalAvg = movieGlobalAvgDeviation.lookup(movieId).head
    val scale = scaleStandardization(userAVG + movieGlobalAvg, userAVG)
    userAVG + movieGlobalAvg * scale
  }
}
