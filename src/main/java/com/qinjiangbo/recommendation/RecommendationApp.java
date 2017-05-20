package com.qinjiangbo.recommendation;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @date: 19/05/2017 4:25 PM
 * @author: qinjiangbo@github.io
 */
public class RecommendationApp {

    private static final String BASE_PATH =
            "/Users/Richard/Documents/SparkML-DataSet/ml-100k/";

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Recommendation App")
                .master("local[2]")
                .getOrCreate();

        JavaSparkContext sparkContext =
                new JavaSparkContext(sparkSession.sparkContext());
    }

}
