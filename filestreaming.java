package com.spark.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class filestreaming {

	public static void main(String[] args) throws StreamingQueryException {
		// TODO Auto-generated method stub
		
		System.setProperty("hadoop.home.dir", "D:\\winutils");
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		
		SparkSession spark = SparkSession.builder()
										 .appName("Streamin")
										 .master("local")
										 .getOrCreate();
		
		StructType userSchema  = new StructType().add("name" ,"String").add("score" ,"int");
		
		Dataset<Row>  stockData = spark
								.readStream()
								.option("sep", ",")
								.schema(userSchema)
								.csv("D:\\Code_BigData\\sparkJava\\workspace\\project1\\src\\main\\resources\\source")
								;
		
		
		 Dataset<Row> resultDf = stockData.groupBy("name").sum("score");
		 
		 StreamingQuery query = resultDf.writeStream()
				 						.outputMode("complete")
				 						.format("console")
				 						.start();
		 
		 query.awaitTermination();
		

	}

}
