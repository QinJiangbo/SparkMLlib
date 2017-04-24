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
        System.out.println(javaRDD.first());
        System.out.println("Total records: " + javaRDD.count());

        sparkSession.stop();
    }
}
