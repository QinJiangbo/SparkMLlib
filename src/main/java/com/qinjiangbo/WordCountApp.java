package com.qinjiangbo;

import com.qinjiangbo.utils.PathUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @date: 19/04/2017 5:00 PM
 * @author: qinjiangbo@github.io
 */
public class WordCountApp {

    public static void main(String[] args) {

        // initialize spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Word Count App")
                // local[N] | spark://** | mesos|zk://** | simr://**
                .master("local[2]")
                .getOrCreate();

        // initialize spark context
        JavaSparkContext sparkContext =
                new JavaSparkContext(sparkSession.sparkContext());

        // we take the raw data in CSV format and convert it into
        // a set of records of the form (user, product, price)

        String filePath = PathUtil.DATASET_PATH + "UserPurchaseHistory.csv";

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
        JavaPairRDD<String, Integer> pairs = data
                .mapToPair(strings -> new Tuple2<>(strings[1], 1));
        List<Tuple2<String, Integer>> list = pairs
                .reduceByKey((a, b) -> a+b).collect();

        // sort list
        list = new ArrayList<>(list);
        Collections.sort(list, (o1, o2) -> -(o1._2 - o2._2));
        String mostPopular = list.get(0)._1();
        int purchases = list.get(0)._2();

        System.out.println("Total purchases: " + numPurchases);
        System.out.println("Unique users: " + uniqueUsers);
        System.out.println("Total revenue: " + totalRevenue);
        System.out.println(String.format("Most popular product: %s with " +
                "%d purchases", mostPopular, purchases));

        // stop session
        sparkSession.stop();
    }
}
