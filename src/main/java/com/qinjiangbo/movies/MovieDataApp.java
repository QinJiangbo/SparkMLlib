package com.qinjiangbo.movies;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @date: 24/04/2017 9:45 AM
 * @author: qinjiangbo@github.io
 */
public class MovieDataApp {

    private static final String BASE_PATH =
            "/Users/Richard/Documents/SparkML-DataSet/ml-100k/";

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

        sparkSession.stop();
    }
}
