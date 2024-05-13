package app.recommender

import app.recommender.LSH.{LSHIndex, NNLookup}
import app.recommender.baseline.BaselinePredictor
import app.recommender.collaborativeFiltering.CollaborativeFiltering
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Class for performing recommendations
 */
class Recommender(sc: SparkContext, index: LSHIndex, ratings: RDD[(Int, Int, Option[Double], Double, Int)]) extends Serializable {
  private val nn_lookup = new NNLookup(index)
  private val collaborativePredictor = new CollaborativeFiltering(10, 0.1, 0, 4)
  collaborativePredictor.init(ratings)

  private val baselinePredictor = new BaselinePredictor()
  baselinePredictor.init(ratings)


  /**
   * Returns the top K recommendations for movies similar to the List of genres
   * for userID using the BaseLinePredictor
   */
  def recommendBaseline(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val similar_moviesID = nn_lookup.lookup(sc.parallelize(Seq(genre))).first()._2.map(_._1)
    val ratedMovies = ratings.filter(_._1 == userId).map(_._2).collect()
    val unratedSimilarMovies = similar_moviesID.diff(ratedMovies)
    val movie_prediction = unratedSimilarMovies.map(x => (x, baselinePredictor.predict(userId, x)))
    movie_prediction.sortWith(_._2 > _._2).take(K)
  }

  /**
   * The same as recommendBaseline, but using the CollaborativeFiltering predictor
   */
  def recommendCollaborative(userId: Int, genre: List[String], K: Int): List[(Int, Double)] = {
    val similar_moviesID = nn_lookup.lookup(sc.parallelize(Seq(genre))).first()._2.map(_._1)
    val ratedMovies = ratings.filter(_._1 == userId).map(_._2).collect()
    val unratedSimilarMovies = similar_moviesID.diff(ratedMovies)
    val movie_prediction = unratedSimilarMovies.map(x => (x, collaborativePredictor.predict(userId, x)))
    movie_prediction.sortWith(_._2 > _._2).take(K)
  }
}
