package com.streaming.spark;


import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class SparkCsvToJson {

    public static Dataset<String> buildDataSet() throws InterruptedException {
        SparkSession spark = SparkSession
                .builder()
                .appName("SparkCsvToJson Service")
                .config("spark.master", "local")
                .getOrCreate();

        StructType schema = new StructType()
                .add("MMSI", "string", true)
                .add("BaseDateTime", "string", true)
                .add("LAT", "string", true)
                .add("LON", "string", true)
                .add("SOG", "string", true)
                .add("COG", "string", true)
                .add("Heading", "string", true)
                .add("VesselName", "string", true)
                .add("IMO", "string", true)
                .add("CallSign", "string", true)
                .add("VesselType", "string", true)
                .add("Status", "string", true)
                .add("Length", "string", true)
                .add("Width", "string", true)
                .add("Draft", "string", true)
                .add("Cargo", "string", true);

        Dataset<String> ds = spark.read().format("csv")
                .options(Map.of("mode", "append", "header", "true"))
                .schema(schema)
                .load("c:/tmp/source/*.csv")
                .toJSON();

        return ds;

    }

    public static void main(String args []) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\softwares\\winutils");
        Dataset<String> ds = SparkCsvToJson.buildDataSet();
        System.out.println("**************** START *******************");
        //ds.show(100, false);
        ds.foreach((ForeachFunction<String>) row -> System.out.println(row));
        System.out.println("**************** END *******************");
    }
}
