package org.learning.spark.ivy.sample;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

/**
 * Sample Spark application that counts the words in a text file
 */
public class WordCount {

	public static void wordCountJava7(String filename) {
		// Define a configuration to use to interact with Spark
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Work Count App");

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext context = new SQLContext(sc);

		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = sc.textFile(filename);

		// Java 7 and earlier
		JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(s.split(" "));
			}
		});

		// Java 7 and earlier: transform the collection of words into pairs (word and 1)
		JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {

				return new Tuple2<String, Integer>(word, 1);
			}
		});

		// Java 7 and earlier: count the words
		JavaPairRDD<String, Integer> reducedCounts = counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer x, Integer y) {
				return x + y;
			}
		});
		// Save the word count back out to a text file, causing evaluation.
		reducedCounts.repartition(1).saveAsTextFile("output");

	}

	public static void wordCountJava8(String filename) {
		// Define a configuration to use to interact with Spark
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data, which is a text file read from the command line
		JavaRDD<String> input = sc.textFile(filename);

		// Java 8 with lambdas: split the input string into words
		JavaRDD<String> words = input.flatMap(s -> Arrays.asList(s.split(" ")));

		// Java 8 with lambdas: transform the collection of words into pairs (word and 1) and then count them
		JavaPairRDD<Object, Object> counts = words.mapToPair(t -> new Tuple2(t, 1)).reduceByKey((x, y) -> (int) x + (int) y);

		// Save the word count back out to a text file, causing evaluation.
		counts.saveAsTextFile("output");
	}

	public static void arrayDFF() {
		// Define a configuration to use to interact with Spark
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Create a simple RDD containing 4 numbers.
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4);
		JavaRDD<Integer> numbersListRdd = sc.parallelize(numbers);
		// Convert this RDD into CSV.
		CSVPrinter printer;
		try {
			printer = new CSVPrinter(System.out, CSVFormat.DEFAULT);

			printer.printRecord(numbersListRdd.collect());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		sc.stop();
	}

	public static void main(String[] args) {

		/*
		 * if (args.length == 0) { System.out.println("Usage: WordCount <file>"); System.exit(0); }
		 */
		System.out.println(args[0]);
		// arrayDFF();
		// wordCountJava7(args[0]);
		// CSVReader.csvExample(args[0]);
		CSVReader.excelExample(args[0]);
	}
}