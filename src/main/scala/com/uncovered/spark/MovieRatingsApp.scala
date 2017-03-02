package com.uncovered.spark

import com.uncovered.spark.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object MovieRatingsApp {

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("No arguments!!! Use <inputPath> <outputFolderPath> <format>")

      return
    }

    val input_path = args(0)
    val output_path = args(2)

    val sc = getSparkContext()

    val movieSourceRdd = sc.textFile(input_path + "/movies")
    val ratingSourceRdd = sc.textFile(input_path + "/rating")

    // Remove headers
    val movieFilteredRdd = returnRemovedHeaders(movieSourceRdd, "movieId")
    val ratingFilteredRdd = returnRemovedHeaders(ratingSourceRdd, "userId")

    // Map each line to the correct object and creates a pair (id, rdd)
    val moviesPairRdd = movieFilteredRdd
      .map(MovieCreator.lineToMovie(_))
      .map(movie => (movie.id, movie))
    val ratingPairRdd = ratingFilteredRdd
      .map(RatingCreator.lineToRating(_))
      .map(rating => (rating.movieId, rating))

    // Persist in cache
    moviesPairRdd.persist(StorageLevel.MEMORY_AND_DISK)
    ratingPairRdd.persist(StorageLevel.MEMORY_AND_DISK)

    // Creates the movie rating dataset
    val movieRatingRdd = getMovieRating(moviesPairRdd, ratingPairRdd)

    val format = args(2)
    if (format.toLowerCase == "text") {
      //output rdd disk as text file
      moviesPairRdd.values.saveAsTextFile(output_path + "/movies")
      ratingPairRdd.values.saveAsTextFile(output_path + "/ratings")

      movieRatingRdd.saveAsTextFile(output_path + "/movie_ratings")
    } else if (format.toLowerCase == "parquet") {
      val sqlCtx = new SQLContext(sc)
      import sqlCtx.implicits._

      val sqlContext = new SQLContext(sc)
      val newMoviesRDD = movieFilteredRdd.map(MovieCreator.lineToMovie(_)).toDF()
      newMoviesRDD.write.parquet(output_path + "/parquet/movies")
      ratingPairRdd.values.toDF().write.parquet(output_path + "/parquet/rating")
      movieRatingRdd.toDF().write.parquet(output_path + "/parquet/movie_ratings")
    } else {
      println("Invalid write format")
    }
  }

  def getSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("MovielensApp")

    new SparkContext(conf)
  }

  def returnRemovedHeaders(rdd: RDD[String], firstHeaderField: String): RDD[String] = {
    rdd.filter((line: String) => !(line startsWith firstHeaderField))
  }

  def getMovieRating(movieRdd: RDD[(Int, Movie)], ratingPairRdd: RDD[(Int, Rating)]): RDD[MovieRating] = {
    import scala.math.{min, max}

    val combinedMovieRating = ratingPairRdd combineByKey(
      (r: Rating) => MovieRating(r.movieId, "", 0, r.rating, 1, r.timestamp, r.timestamp),
      (m: MovieRating, r: Rating) => MovieRating(m.movieId, "", 0,
        r.rating + m.total_rating,
         m.no_rating + 1,
         min(r.timestamp, m.first_rated_ts),
         max(r.timestamp, m.last_rated_ts)),
      (m: MovieRating,n:MovieRating) => MovieRating(m.movieId, "",
         (m.total_rating + n.total_rating)/ (m.no_rating + n.no_rating),
         m.total_rating + n.total_rating, m.no_rating + n.no_rating,
         min(m.first_rated_ts, n.first_rated_ts),
         max(m.last_rated_ts, n.last_rated_ts)))

    val movieJoinRatingRdd = movieRdd leftOuterJoin combinedMovieRating
    val mappedMovieJoinRatingRdd = movieJoinRatingRdd.map(
      tuple => tuple._2 match {
        case (m, None) => MovieRating(m.id, m.title, 0.0F, 0.0F, 0, 0L, 0L)
        case (m, Some(c)) => MovieRating(m.id, m.title,  c.avg_rating, c.total_rating,
          c.no_rating, c.first_rated_ts, c.last_rated_ts)
      }
    )

    mappedMovieJoinRatingRdd
  }
}