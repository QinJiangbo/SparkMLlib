package com.qinjiangbo.movies;

import com.qinjiangbo.utils.PathUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @date: 23/04/2017 5:53 PM
 * @author: qinjiangbo@github.io
 */
public class MovieUserApp {

    private static final String BASE_PATH = PathUtil.DATASET_PATH + "ml-100k/";

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Movie Lens App")
                .master("local[2]")
                .getOrCreate();

        JavaSparkContext sparkContext =
                new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> users = sparkContext.textFile(BASE_PATH + "u.user");
        JavaRDD<String[]> data = users.map(string -> string.split("\\|"));

        // extract fields
        long userNums = data.map(strings -> strings[0]).count();
        long sexNums = data.map(strings -> strings[2]).distinct().count();
        long occupNums = data.map(strings -> strings[3]).distinct().count();
        long zipCodeNums = data.map(strings -> strings[4]).distinct().count();

        System.out.println("UserNum=" + userNums +
                "\nSexNum=" + sexNums +
                "\nOccupationNum=" + occupNums +
                "\nZipCodeNum=" + zipCodeNums
        );

        sparkSession.stop();

    }
}
