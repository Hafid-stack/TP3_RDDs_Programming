package com.enset;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
public class App2 {
    public static void main(String[] args) {
        // 1. Configure Spark
        SparkConf conf = new SparkConf().setAppName("Total Sales by Product, City, and Year").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // 2. Read the data file
        JavaRDD<String> linesRDD = sc.textFile("ventes.txt");

        // 3. Map to a ( (product, city, year), price ) pair
        JavaPairRDD<Tuple2<String, String>, Double> salesData = linesRDD.mapToPair(line -> {
            String[] parts = line.split(" ");
            String date = parts[0];
            String year = date.substring(0, 4); // Extract the year from the date
            String city = parts[1];
            String product = parts[2];
            double price = Double.parseDouble(parts[3]);

            // Create a compound key (product, city)
            Tuple2<String, String> key = new Tuple2<>(product, city);
            return new Tuple2<>(key, price);
        });

        // 4. Reduce by the compound key to sum the prices
        JavaPairRDD<Tuple2<String, String>, Double> totalSales = salesData.reduceByKey((total, current) -> total + current);

        // 5. Print the results
        System.out.println("Total Sales by Product, City, and Year:");
        totalSales.foreach(result -> {
            String product = result._1()._1();
            String city = result._1()._2();
            double total = result._2();
            System.out.println("Product: " + product + ", City: " + city + ", Total: " + total + " DH");
        });

        // 6. Stop Spark context
        sc.stop();
    }
}
