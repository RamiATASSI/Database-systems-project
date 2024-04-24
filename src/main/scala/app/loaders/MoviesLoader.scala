package app.loaders

import app.Main.getClass
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc   The Spark context for the given application
 * @param path The path for the input file
 */
class MoviesLoader(sc: SparkContext, path: String) extends Serializable {

  /**
   * Read the title file in the given path and convert it into an RDD
   *
   * @return The RDD for the given titles
   */
  def load(): RDD[(Int, String, List[String])] = {
    val resource = getClass.getResource("/dataset_3/" + path)
    if (resource == null) {
      throw new IllegalArgumentException(s"Resource not found: /dataset_3/$path")
    }
    val full_path = new File(resource.getFile).getPath
    val text_lines = sc.textFile(full_path)
    val movies = text_lines.map { line =>
      val fields = line.replace("\"", "").trim.split('|')
      val movieId = fields(0).toInt
      val title = fields(1)
      val genres = fields.slice(2, fields.length).toList
      (movieId, title, genres)
    }
    movies.persist()
  }
}

