package com.qinjiangbo.movies;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @date: 23/04/2017 6:57 PM
 * @author: qinjiangbo@github.io
 */
public class MovieItemApp {

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

        JavaRDD<String> items = sparkContext.textFile(BASE_PATH + "u.item");
        System.out.println("Movies: " + items.count());

        JavaRDD<String[]> data = items.map(string -> string.split("\\|"));
        data = data.filter(strings -> strings[2].split("-").length == 3);
        JavaRDD<String[]> years = data.map(strings -> strings[2].split("-"));
        JavaPairRDD<String, Integer> pairs = years
                .mapToPair(strings -> new Tuple2<>(strings[2], 1));
        List<Tuple2<String, Integer>> list = pairs
                .reduceByKey((a , b) -> a + b).collect();

        list = new ArrayList<>(list);
        Collections.sort(list, (o1, o2) -> -(o1._2 - o2._2));
        String year = list.get(0)._1;
        int count = list.get(0)._2;

        System.out.println("Popular Year: " + year + ", number: " + count);

        sparkSession.stop();
    }
}
