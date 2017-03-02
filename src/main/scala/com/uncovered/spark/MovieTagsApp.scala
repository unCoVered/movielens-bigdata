package com.uncovered.spark

import com.uncovered.spark.model._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

object MovieTagsApp {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("No arguments!!! Use <inputPath> <outputFolderPath> <format>")
      return;
    }

    val input_path = args(0)
    val output_path = args(1)

    val sc = getSparkContext()

    val movieSourceRdd = sc.textFile(input_path + "/movies")
    val tagSourceRdd = sc.textFile(input_path + "/tags")

    //remove headers
    val movieFilteredRdd = returnRemoveHeaders(movieSourceRdd, "movieId")
    val tagsFilteredRdd = returnRemoveHeaders(tagSourceRdd, "userId")

    //map to objects
    val moviesRDD = movieFilteredRdd map (MovieCreator.lineToMovie(_))
    val tagsRDD = tagsFilteredRdd map (TagCreator.lineToTag(_))

    //Pair id - object
    val moviePairRdd = moviesRDD map (movie => (movie.id, movie))
    val movieTagPairRdd = tagsRDD map (tag => (tag.movieId, tag))

    //Group tags
    val movieTagsRdd = movieTagPairRdd groupByKey()

    //Persists
    moviePairRdd.persist(StorageLevel.MEMORY_AND_DISK)
    movieTagsRdd.persist(StorageLevel.MEMORY_AND_DISK)

    //Create data sets
    val movieVsTagsRdd = getMovieTags(moviePairRdd, movieTagsRdd)
    val tagVsMoviesRdd = getTagMovies(movieTagsRdd)

    val format = args(2)

    if (format.toLowerCase == "text"){
      //output rdd to disk
      tagsRDD.saveAsTextFile(output_path + "/tags")
      movieTagsRdd.saveAsTextFile(output_path + "/movietags")
      movieVsTagsRdd.saveAsTextFile(output_path + "/movies_tags")
      tagVsMoviesRdd.saveAsTextFile(output_path + "/tags_movies")
    } else if (format.toLowerCase == "parquet"){
      val sqlCtx = new SQLContext(sc)
      import sqlCtx.implicits._ //unleashing the encoders and types

      //output rdd to disk as parquet
      tagsRDD.toDF().write.parquet(output_path + "/tags")
      movieTagsRdd.toDF().write.parquet(output_path + "/movietags")
      movieVsTagsRdd.toDF().write.parquet(output_path + "/parquet/movie_ratings")
      tagVsMoviesRdd.toDF().write.parquet(output_path + "/parquet/rating")
    } else {
      println("Invalid write format")
    }
  }

  def getSparkContext(): SparkContext = {
    val conf = new SparkConf().setAppName("MovielensApp")

    new SparkContext(conf)
  }

  def returnRemoveHeaders(rdd: RDD[String], firstHeaderString: String): RDD[String] = {
    rdd.filter((line: String) => !(line startsWith firstHeaderString))
  }

  def getMovieTags(movieRdd: RDD[(Int, Movie)], movieTagsRdd: RDD[(Int, Iterable[Tag])]): RDD[MovieTags] = {
    val movieJoinGrpKeyRdd = movieRdd leftOuterJoin movieTagsRdd

    movieJoinGrpKeyRdd map (tuple => tuple._2 match {
      case (m: Movie, None) => new MovieTags(tuple._1, m.title, List())
      case (m: Movie, Some(a)) =>
        new MovieTags(tuple._1, m.title, a.flatMap(_.tag.split(" ")).filter(_.length > 1).toList)
    })
  }

  def getTagMovies(moviesTagsRdd: RDD[(Int, Iterable[Tag])]): RDD[TagMovies] = {
    //Creates RDD pairing movieIds with tags
    val tagMovieIdPair: RDD[(String, Int)] =
      moviesTagsRdd flatMap(mtr => for (t <- mtr._2) yield (t.tag.toLowerCase(), mtr._1))

    tagMovieIdPair.groupByKey() map (pair => TagMovies(pair._1, pair._2.toList))
  }
}