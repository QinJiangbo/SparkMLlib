package com.qinjiangbo.movies;

import com.qinjiangbo.utils.PathUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;

/**
 * @date: 24/04/2017 9:45 AM
 * @author: qinjiangbo@github.io
 */
public class MovieDataApp {

    private static final String BASE_PATH = PathUtil.DATASET_PATH + "ml-100k/";

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Movie Lens App")
                .master("local[2]")
                .getOrCreate();

        JavaSparkContext sparkContext =
                new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> javaRDD = sparkContext.textFile(BASE_PATH + "u.data");
        JavaRDD<String[]> data = javaRDD.map(string -> string.split("\t"));
        long userNums = data.map(strings -> strings[0]).distinct().count();
        long movieNums = data.map(strings -> strings[1]).distinct().count();
        JavaRDD<Integer> ratings = data.map(strings -> Integer.valueOf(strings[2]));
        long maxRating = ratings.reduce((a, b) -> Math.max(a, b));
        long minRating = ratings.reduce((a, b) -> Math.min(a, b));
        long ratingNums = ratings.count();
        double meanRating = ratings.reduce((a, b) -> (a + b)) / (double) ratingNums;
        long ratingsPerUser = ratingNums / userNums;
        long ratingsPerMovie = ratingNums / movieNums;

        System.out.println("Min rating: " + minRating);
        System.out.println("Max rating: " + maxRating);
        System.out.println("Average rating: " + meanRating);
        System.out.println("Average # of ratings per user: " + ratingsPerUser);
        System.out.println("Average # of ratings per movie: " + ratingsPerMovie);

        // user ratings by user
        JavaPairRDD<String, Integer> userPairs = data
                .mapToPair(strings -> new Tuple2<>(strings[0], 1));
        List<Tuple2<String, Integer>> uPairList =
                userPairs.reduceByKey((a, b) -> a + b).collect();
        System.out.println("user: " + uPairList.get(0)._1 + " - " + uPairList.get(0)._2);

        // movie ratings by movie
        JavaPairRDD<String, Integer> moviePairs = data
                .mapToPair(strings -> new Tuple2<>(strings[1], 1));
        List<Tuple2<String, Integer>> mPairList =
                moviePairs.reduceByKey((a, b) -> a + b).collect();
        System.out.println("movie: " + mPairList.get(0)._1 + " - " + mPairList.get(0)._2);

        sparkSession.stop();
    }
}
