val first_rdd = sc.parallelize(1 to 1000)
first_rdd.map(_*2).reduce(_+_) 

val doubled = first_rdd.map( _*2)
doubled.take(10)

val movie_raw = sc.textFile(“./sample_data/ml-1m/movies.dat")
movie_raw.first

case class Movie( Id: Int, Title: String, Genres: String)
val movie_rdd = movie_data.map( r => r.split("::")).map( r => Movie(r(0).toInt, r(1), r(2))
movie_rdd.first

val ratings_raw = sc.textFile(“./sample_data/ml-1m/ratings.dat")
ratings_raw.first

case class Rating(UserId: Int, MovieId: Int, Rating: Float)
val ratings_rdd = ratings_raw.map( r => r.split("::")).map( r => Rating(r(0).toInt, r(1).toInt, r(2).toFloat)
ratings_rdd.first

import org.apache.spark.sql.SQLContext
val sqlContext = new SQLContext(sc)
import sqlContext.createSchemaRDD

movie_rdd.registerTempTable("movies")
ratings_rdd.registerTempTable(“ratings”)

sqlContext.sql(“SELECT * FROM movies”)

sqlContext.sql("SELECT ratings.UserId, movies.Title, ratings.Rating FROM ratings JOIN movies ON movies.Id = ratings.MovieId").take(10)


sqlContext.sql("SELECT ratings.UserId, movies.Title, ratings.Rating FROM ratings JOIN movies ON movies.Id = ratings.MovieId WHERE ratings.Rating = 5”).take(10)
