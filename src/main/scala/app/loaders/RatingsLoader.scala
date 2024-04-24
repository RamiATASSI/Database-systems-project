package app.loaders

import app.Main.getClass
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

/**
 * Helper class for loading the input
 *
 * @param sc The Spark context for the given application
 * @param path The path for the input file
 */
class RatingsLoader(sc : SparkContext, path : String) extends Serializable {

  /**
   * Read the rating file in the given path and convert it into an RDD
   *
   * @return The RDD for the given ratings
   */
  def load() : RDD[(Int, Int, Option[Double], Double, Int)] = {
    val resource = getClass.getResource(path)
    if (resource == null) {
      throw new IllegalArgumentException(s"Resource not found: $path")
    }
    val full_path = new File(resource.getFile).getPath
    val ratings = sc.textFile(full_path).map { line =>
      val fields = line.split('|')
      fields.length match {
        case 4 => (fields(0).toInt, fields(1).toInt, None, fields(2).toDouble, fields(3).toInt)
        case 5 => (fields(0).toInt, fields(1).toInt, Option(fields(2).toDouble), fields(3).toDouble, fields(4).toInt)
      }
    }
    ratings.persist()
  }
}