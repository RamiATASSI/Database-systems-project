package app.analytics

import org.apache.spark.HashPartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK
import org.joda.time.DateTime


class SimpleAnalytics() extends Serializable {

  private var ratingsPartitioner: HashPartitioner = _
  private var moviesPartitioner: HashPartitioner = _
  private var titlesGroupedById: RDD[(Int, Iterable[(Int, String, List[String])])] = _
  private var ratingsGroupedByYearByTitle: RDD[((Int, Int), Iterable[(Int, Int, Option[Double], Double, Int)])] = _


  def init(
            ratings: RDD[(Int, Int, Option[Double], Double, Int)],
            movie: RDD[(Int, String, List[String])]
          ): Unit = {
    moviesPartitioner = new HashPartitioner(movie.partitions.length)
    titlesGroupedById = movie.groupBy(_._1).partitionBy(moviesPartitioner).persist(MEMORY_AND_DISK)

    ratingsPartitioner = new HashPartitioner(ratings.partitions.length)
    ratingsGroupedByYearByTitle = ratings.groupBy(rdd => {
      val year = new DateTime(rdd._5 * 1000L).getYear
      (year, rdd._2)
    }).partitionBy(ratingsPartitioner).persist(MEMORY_AND_DISK)
  }

  def getNumberOfMoviesRatedEachYear: RDD[(Int, Int)] = ratingsGroupedByYearByTitle.map {
    case ((year, title), ratings) => (year, 1)
  }.reduceByKey(_ + _)

  private def getMostRatedMovieIdEachYear: RDD[(Int, Int)] = {
    val titleID_year = ratingsGroupedByYearByTitle.map {
        case ((year, titleID), ratings) => (year, (titleID, ratings.size))
      }.reduceByKey((a, b) =>
        if (a._2 > b._2) a
        else if (a._2 < b._2) b
        else if (a._1 > b._1) a
        else b
      )
      .map {
        case (year, (titleID, nbr_ratings)) => (titleID, year)
      }
    titleID_year
  }

  def getMostRatedMovieEachYear: RDD[(Int, String)] = {

    val titleID_year = getMostRatedMovieIdEachYear

    val titleID_title = titlesGroupedById.map {
      case (titleID, iterable) => (titleID, iterable.head._2)
    }

    titleID_year.join(titleID_title).map {
      case (titleID, (year, title)) => (year, title)
    }
  }

  def getMostRatedGenreEachYear: RDD[(Int, List[String])] = {

    val titleID_year = getMostRatedMovieIdEachYear

    val titleID_genres = titlesGroupedById.map {
      case (titleID, iterable) => (titleID, iterable.head._3)
    }

    titleID_year.join(titleID_genres).map {
      case (titleID, (year, genres)) => (year, genres)
    }
  }

  // Note: if two genre has the same number of rating, return the first one based on lexicographical sorting on genre.
  def getLeastAndMostRatedGenreAllTime: ((String, Int), (String, Int)) = {

    val genre_count = getMostRatedGenreEachYear.flatMap {
      case (year, genres) => genres.map(genre => (genre, 1))
    }.reduceByKey(_ + _)

    val most_genre = genre_count.reduce((a, b) =>
      if (a._2 > b._2) a
      else if (a._2 < b._2) b
      else if (a._1 < b._1) a
      else b
    )

    val least_genre = genre_count.reduce((a, b) =>
      if (a._2 < b._2) a
      else if (a._2 > b._2) b
      else if (a._1 < b._1) a
      else b
    )

    (least_genre, most_genre)
  }

  /**
   * Filter the movies RDD having the required genres
   *
   * @param movies         RDD of movies dataset
   * @param requiredGenres RDD of genres to filter movies
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre(movies: RDD[(Int, String, List[String])],
                          requiredGenres: RDD[String]): RDD[String] = {
    val genreSet = requiredGenres.map((_, None))
    movies.flatMap { case (id, title, genres) => genres.map((_, (id, title))) }
      .join(genreSet)
      .map { case (_, ((id, title), _)) => title }
  }

  /**
   * Filter the movies RDD having the required genres
   * HINT: use the broadcast callback to broadcast requiresGenres to all Spark executors
   *
   * @param movies            RDD of movies dataset
   * @param requiredGenres    List of genres to filter movies
   * @param broadcastCallback Callback function to broadcast variables to all Spark executors
   *                          (https://spark.apache.org/docs/2.4.8/rdd-programming-guide.html#broadcast-variables)
   * @return The RDD for the movies which are in the supplied genres
   */
  def getAllMoviesByGenre_usingBroadcast(movies: RDD[(Int, String, List[String])],
                                         requiredGenres: List[String],
                                         broadcastCallback: List[String] => Broadcast[List[String]]): RDD[String] = {
    val genreSet = broadcastCallback(requiredGenres)
    movies.flatMap { case (id, title, genres) => genres.filter(genreSet.value.contains).map((_, (id, title)))
      .map { case (_, (id, title)) => title }
    }
  }
  
}

