package com.kafka.stream.service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class CustomTransformer implements Transformer<String, String, KeyValue<String, String>> {

    private ProcessorContext context;
    private String flag;

    public CustomTransformer(String flag, String s) {
        this.flag = flag;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public KeyValue<String, String> transform(String key, String value) {
        JsonElement root = JsonParser.parseString(value);
        JsonObject jsonObj = (JsonObject) root.getAsJsonObject();
        jsonObj.addProperty("falg", this.flag);
        return new KeyValue(key, new Gson().toJson(jsonObj));
    }

    @Override
    public void close() {
    }
}
