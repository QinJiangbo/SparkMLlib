package com.qinjiangbo.recommendation;

import com.qinjiangbo.utils.PathUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

/**
 * @date: 19/05/2017 4:25 PM
 * @author: qinjiangbo@github.io
 */
public class RecommendationApp {

    private static final String BASE_PATH = PathUtil.DATASET_PATH + "ml-100k/";

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Recommendation App")
                .master("local[2]")
                .getOrCreate();

        JavaSparkContext sparkContext =
                new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> rawData
                = sparkContext.textFile(BASE_PATH + "u.data");

        JavaRDD<String[]> rawStrings = rawData.map(s -> s.split("\t"));
        System.out.println(Arrays.toString(rawStrings.first()));

        sparkSession.stop();
    }

}
