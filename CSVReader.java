package org.learning.spark.ivy.sample;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class CSVReader {

	public static void csvExample(String path) {
		// Define a configuration to use to interact with Spark
		SparkConf conf = new SparkConf().setMaster("local").setAppName("Work Count App");

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext context = new SQLContext(sc);
		DataFrame df = context.read().format("com.databricks.spark.csv").option("header", "true").load(path);
		df.show();
		df.registerTempTable("temp");
		df = context.sql("select name, count*23 as count from temp");
		df.show();
		writeIntoExcel("output.csv", df.collect());

	}

	public static void writeIntoExcel(String file, org.apache.spark.sql.Row[] rows) {
		System.out.println("hihssssssssssssssssssssssssssssssssssssssssssssss");
		File file1 = new File(file);
		try {

			FileWriter fileWriter = new FileWriter(file1);
			fileWriter.write("name,count\n");
			for (org.apache.spark.sql.Row name : rows) {
				fileWriter.write(name.getString(0) + "," + name.getDouble(1) + "\n");
			}
			fileWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Io Exceptions Occurs ");
		}
	}

	public static void writeIntoExcel1(String file, org.apache.spark.sql.Row[] rows) {
		System.out.println("hihssssssssssssssssssssssssssssssssssssssssssssss");
		Workbook book = new HSSFWorkbook();
		Sheet sheet = book.createSheet("Data");
		// header row
		Row row = sheet.createRow(0);

		row.createCell(0).setCellValue("name");
		row.createCell(1).setCellValue("count");

		int i = 1;

		for (org.apache.spark.sql.Row name : rows) {
			Row row1 = sheet.createRow(i);
			row1.createCell(0).setCellValue(name.getString(0));
			row1.createCell(1).setCellValue(name.getDouble(1));
			i++;
		}
		try {

			book.write(new FileOutputStream(file));
			book.close();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("Io Exceptions Occurs ");
		}
	}

	public static void excelExample(String path) {
		// Define a configuration to use to interact with Spark
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Work Count App");

		// Create a Java version of the Spark Context from the configuration
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext context = new SQLContext(sc);
		List<Name> names = null;
		try {
			FileInputStream file = new FileInputStream(new File(path));

			// Create Workbook instance holding reference to .xlsx file
			XSSFWorkbook workbook = new XSSFWorkbook(file);

			// Get first/desired sheet from the workbook
			XSSFSheet sheet = workbook.getSheetAt(0);
			names = new ArrayList<Name>();
			// Iterate through each rows one by one
			Iterator<Row> rowIterator = sheet.iterator();
			rowIterator.next();
			while (rowIterator.hasNext()) {
				Row row = rowIterator.next();
				// For each row, iterate through all the columns
				Iterator<Cell> cellIterator = row.cellIterator();

				Name name = new Name();
				while (cellIterator.hasNext()) {

					Cell cell = cellIterator.next();
					// Check the cell type and format accordingly
					switch (cell.getCellType()) {
					case Cell.CELL_TYPE_NUMERIC:
						System.out.print(cell.getNumericCellValue() + "t");
						name.setCount(cell.getNumericCellValue());
						break;
					case Cell.CELL_TYPE_STRING:
						System.out.print(cell.getStringCellValue());
						name.setName(cell.getStringCellValue());
						break;
					}

				}
				names.add(name);

			}
			System.out.println(names);
			file.close();

		} catch (Exception e) {
			e.printStackTrace();
		}

		DataFrame df = context.createDataFrame(names, Name.class);
		df.show();
		df.registerTempTable("temp");
		df = context.sql("select name, count*23 as count from temp");
		df.show();
		// df.withColumn("countNum", df.col("count")).write().format("com.databricks.spark.csv").save("words.csv");

		writeIntoExcel("output1.csv", df.collect());
	}
}
