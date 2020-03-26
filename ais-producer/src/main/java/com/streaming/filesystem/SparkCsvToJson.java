package com.streaming.filesystem;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.File;

public class SparkCsvToJson {
    public SparkCsvToJson() {

    }

    private Dataset<Row> buildDataSet() {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkCsvToJson Service")
                .getOrCreate();

        StructType schema = new StructType()
                .add("MMSI", "string")
                .add("BaseDateTime", "string")
                .add("LAT", "string")
                .add("LON", "string")
                .add("SOG", "string")
                .add("COG", "string")
                .add("Heading", "string")
                .add("CallSign", "string")
                .add("VesselType", "string")
                .add("Status", "string")
                .add("Width", "string")
                .add("Draft", "string")
                .add("Cargo", "string");

        Dataset<Row> df = spark.read().format("csv")
                .option("mode", "DROPMALFORMED")
                .option("header", "true")
                .schema(schema)
                .load("/tmp/source/*.csv");

        return df;

    }

    public static void main(String args []) {
        System.out.println("directory:"+new File(".").getAbsolutePath());
        SparkCsvToJson obj = new SparkCsvToJson();
        Dataset<Row> row = obj.buildDataSet();
        row.describe();
    }
}
