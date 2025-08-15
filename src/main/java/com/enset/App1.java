package com.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.*;

import java.lang.Double;

public class App1 {
    public static void main(String[] args) {
        // 1. Configure Spark to run locally with all available cores
        SparkConf conf = new SparkConf().setAppName("Total Sales by City").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2. Read the ventas.txt file into an RDD
        JavaRDD<String> linesRDD = sc.textFile("ventes.txt");

        // 3. Map each line to a (city, price) pair
        JavaPairRDD<String, Double> cityPricePairRDD = linesRDD.mapToPair(line -> {
            String[] parts = line.split(" ");
            String city = parts[1];
            double price = Double.parseDouble(parts[3]);
            return new Tuple2<>(city, price);
        });

        // 4. Reduce by key (city) to sum the prices
        JavaPairRDD<String, Double> totalSalesByCity = cityPricePairRDD.reduceByKey((total, current) -> total + current);

        // 5. Print the results to the console
        System.out.println("Total Sales by City:");
        totalSalesByCity.foreach(result -> System.out.println(result._1() + " : " + result._2() + " DH"));

        // 6. Stop the Spark context
        sc.stop();
    }
}

