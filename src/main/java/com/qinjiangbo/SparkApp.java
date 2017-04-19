package com.qinjiangbo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * @date: 19/04/2017 5:00 PM
 * @author: qinjiangbo@github.io
 */
public class SparkApp {

    public static void main(String[] args) {

        // initialize spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark App")
                // local[N] | spark://** | mesos|zk://** | simr://**
                .master("local[2]")
                .getOrCreate();

        // initialize spark context
        JavaSparkContext sparkContext =
                new JavaSparkContext(sparkSession.sparkContext());

        // we take the raw data in CSV format and convert it into
        // a set of records of the form (user, product, price)

        String filePath = System.getProperty("user.dir") +
                "/src/main/resources/UserPurchaseHistory.csv";

        JavaRDD<String[]> data = sparkContext.textFile(filePath)
                .map(s -> s.split(","));

        // let's count the number of purchases
        long numPurchases = data.count();
        // let's count how many unique users made purchases
        long uniqueUsers = data.map(strings -> strings[0]).distinct().count();
        // let's sum up our total revenue
        double totalRevenue = data
                .map(strings -> Double.parseDouble(strings[2]))
                .reduce((a, b) -> a + b).doubleValue();
        // let's find our most popular product
        // first we map the data to records of (product, 1) using a PairFunction
        // and the Tuple2 class.
        // then we call a reduceByKey operation with a Function2,
        // which is essentially the sum function
        List<Tuple2<String, Integer>> pairs = data
                .map(strings -> new Tuple2<>(strings[1], 1))
                .collect();
        pairs.forEach(s-> System.out.println(s._1 + " - " + s._2));

        System.out.println("Total purchases: " + numPurchases);
        System.out.println("Unique users: " + uniqueUsers);
        System.out.println("Total revenue: " + totalRevenue);
    }
}
