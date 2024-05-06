package app.aggregator

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.apache.spark.{HashPartitioner, SparkContext}

/**
 * Class for computing the aggregates
 *
 * @param sc The Spark context for the given application
 */
class Aggregator(sc: SparkContext) extends Serializable {

  private var partitioner: HashPartitioner = _
  private var state: RDD[(Int, (String, List[String], (Double, Int)))] = _


  /**
   * Use the initial ratings and titles to compute the average rating for each title.
   * The average rating for unrated titles is 0.0
   *
   * @param ratings The RDD of ratings in the file
   *                format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   * @param title   The RDD of titles in the file
   */
  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            title: RDD[(Int, String, List[String])]
          ): Unit = {
    partitioner = new HashPartitioner(title.partitions.length)
    val meanRatings = ratings.groupBy(_._2).mapValues(_.map(_._4)).mapValues(x => (x.sum / x.size, x.size))
    state = title.map { case (title_id, title, keywords) => (title_id, (title, keywords)) }
      .leftOuterJoin(meanRatings)
      .map {
        case (titleID, ((title, keywords), avgRating)) =>
          (titleID, (title, keywords, avgRating.getOrElse(0.0, 0)))
      }
      .partitionBy(partitioner).persist(MEMORY_AND_DISK)
  }

  /**
   * Return pre-computed title-rating pairs.
   *
   * @return The pairs of titles and ratings
   */
  def getResult(): RDD[(String, Double)] = state.map { case (_, (title, _, (mean, _))) => (title, mean) }

  /**
   * Compute the average rating across all (rated titles) that contain the
   * given keywords.
   *
   * @param keywords A list of keywords. The aggregate is computed across
   *                 titles that contain all the given keywords
   * @return The average rating for the given keywords. Return 0.0 if no
   *         such titles are rated and -1.0 if no such titles exist.
   */
  def getKeywordQueryResult(keywords: List[String]): Double = {
    var moviesWithKeywords = state.filter {
      case (_, (_, movieKeywords, _)) => keywords.forall(movieKeywords.contains)
    }
    if moviesWithKeywords.isEmpty() then return -1.0
    moviesWithKeywords = moviesWithKeywords.filter(_._2._3._2 > 0)
    if moviesWithKeywords.isEmpty() then return 0.0
    moviesWithKeywords.map(_._2._3._1).sum / moviesWithKeywords.count
  }

  /**
   * Use the "delta"-ratings to incrementally maintain the aggregate ratings
   *
   * @param delta Delta ratings that haven't been included previously in aggregates
   *              format: (user_id: Int, title_id: Int, old_rating: Option[Double], rating: Double, timestamp: Int)
   */
  def updateResult(delta_ : Array[(Int, Int, Option[Double], Double, Int)]): Unit = {
    val deltaRatings = sc.parallelize(delta_)

    val newRatings = deltaRatings.groupBy(_._2).mapValues(ratings => {
      val sum = ratings.map { case (_, _, oldRating, newRating, _) => newRating - oldRating.getOrElse(0.0) }.sum
      val count = ratings.count(r => r._3.isEmpty)
      (sum, count)
    })

    val updatedAvgRatingByTitle = state.leftOuterJoin(newRatings).map {
      case (titleID, ((title, keywords, (oldMean, oldSize)), None)) =>
        (titleID, (title, keywords, (oldMean, oldSize)))
      case (titleID, ((title, keywords, (oldMean, oldSize)), Some((newSum, newSize)))) =>
        val totalSum = oldMean * oldSize + newSum
        val totalCount = oldSize + newSize
        (titleID, (title, keywords, (totalSum / totalCount, totalCount)))
    }

    state.unpersist()
    state = updatedAvgRatingByTitle.partitionBy(partitioner).persist(MEMORY_AND_DISK)
  }
}
