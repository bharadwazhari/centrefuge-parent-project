package com.kafka.stream.service;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueMapper;

public class CustomValueMapper implements ValueMapper<String, String> {

    private String flag;

    public CustomValueMapper(String flag) {
        this.flag = flag;
    }

    @Override
    public String apply(String value) {
        JsonElement root = JsonParser.parseString(value);
        JsonObject jsonObj = (JsonObject) root.getAsJsonObject();
        jsonObj.addProperty("falg", this.flag);
        return new Gson().toJson(jsonObj);
    }
}
