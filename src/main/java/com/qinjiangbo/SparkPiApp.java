package com.qinjiangbo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

/**
 * @date: 23/04/2017 10:08 AM
 * @author: qinjiangbo@github.io
 */
public class SparkPiApp {

    public static void main(String[] args) {

        // initialize the spark session
        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Spark Pi App")
                .master("local[2]")
                .getOrCreate();

        JavaSparkContext sparkContext =
                new JavaSparkContext(sparkSession.sparkContext());

        int slices = 2;
        int n = 1000000 * slices;
        List<Integer> list = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            list.add(i);
        }

        // Java RDD
        JavaRDD<Integer> dataSet = sparkContext.parallelize(list, slices);

        // map-reduce
        int count = dataSet.map(int1 -> {
            double x = Math.random() * 2 - 1;
            double y = Math.random() * 2 - 1;
            return (x * x + y * y <= 1) ? 1 : 0;
        }).reduce((int1, int2) -> int1 + int2);

        // count the numbers in circle
        System.out.println("Pi is roughly " + 4.00 * count / n);

        // stop spark
        sparkSession.stop();

    }
}
