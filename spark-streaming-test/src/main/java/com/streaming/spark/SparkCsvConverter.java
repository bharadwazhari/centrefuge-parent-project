package com.streaming.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkCsvConverter {

    public static void main(String args []) throws IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\softwares\\winutils");
        SparkCsvConverter converter = new SparkCsvConverter();
        SparkSession sparkSession = converter.prepareSparkSession();
        StructType schema = converter.prepareSchema();
        Dataset<Row> ds = converter.convertToDataset(sparkSession, schema, "c:/tmp/source/*.csv");
        Dataset<String> recordsSet = converter.convertToTarget(sparkSession, ds, "avro");
        recordsSet.show(false);
        converter.publish(recordsSet);
    }

    private void publish(Dataset<String> set) {
        set.foreachPartition(partitionOfRecords -> {
            Producer<Integer, String> producer = KafkaProducerClient.getProducer();
            while (partitionOfRecords.hasNext()) {
                producer.send(new ProducerRecord<>("test", null, partitionOfRecords.next()));
            }
        });
    }

    private Dataset<String> convertToTarget(SparkSession sparkSession, Dataset<Row> ds, String format) throws IOException {
        Dataset<String> data = ds.toJSON();
        if("json".equals(format)) return data;
        Stream<String> stream = data.collectAsList().stream().map(item -> StringUtils.replace(avroSchema, "#content#", item));
        List<String> list = stream.collect(Collectors.toList());
        //JavaDStream<String> lines =
        data = sparkSession.createDataset(list, Encoders.STRING());
        return data;
    }

    private Dataset<String> convertToAvro(Dataset<Row> ds, String schema) throws IOException {
        Dataset<String> avroDataSet = null;
        File file = new File(getClass().getResource("data-capsule.avsc").getFile());
        JsonNode mySchema = JsonLoader.fromFile(file);
        String avroSchema = mySchema.asText();
        return avroDataSet;
    }

    private Dataset<Row> convertToDataset(SparkSession spark, StructType schema, String dir) {
        Dataset<Row> ds =  spark.read().format("csv")
                                .options(Map.of("mode", "append", "header", "true"))
                                .schema(schema)
                                .load(dir);
        return ds;
    }

    private SparkSession prepareSparkSession() throws InterruptedException {
        SparkSession spark = SparkSession
                                .builder()
                                .appName("Spark Csv To OtherFormats Service")
                                .config("spark.master", "local")
                                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaStreamingContext jsc = new JavaStreamingContext(sc, new Duration(10000));
        jsc.start();
        jsc.awaitTermination();
        return spark;
    }

    private StructType prepareSchema() {
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
        return schema;
    }

    private static final String avroSchemaPart1 = "{ \"contentype\" :  \"application/json\"," +
            "        \"createdtimestamp\" : ";
    private static final String avroSchemaPart2 = ",\"content\" :";

    private static final String avroSchema = avroSchemaPart1 + "\""+
                                             new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(new Date()) +
                                             "\" "+ avroSchemaPart2 + "#content#" + "}";
}
