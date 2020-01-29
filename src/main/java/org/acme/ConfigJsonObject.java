package org.acme;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.MalformedJsonException;

/**
 *
 * @author Magnus
 */
public class ConfigJsonObject {

    Gson gson = new Gson();
    private ArrayList<Condition> conditionsList = new ArrayList<>();
    private Map<String, Object> jsonMap;
    private final String moduleName;
    private Date date = new Date();
    private long time = date.getTime();
    private Timestamp ts = new Timestamp(time);
    private JsonParser jParser = new JsonParser();
    public String jsonObjectString;

    public ConfigJsonObject(String moduleName) {
        this.moduleName = moduleName;
    }

    public String getJsonObjectString() {
        return jsonObjectString;
    }

    public void addJsonObject(String key, Object value) {
        JsonObject obj = jParser.parse(jsonObjectString).getAsJsonObject();
        obj.add(key, jParser.parse(value.toString()));
        jsonObjectString = obj.toString();
    }

    public Condition getHighestPriorityCondition() {
        Condition maxPriorityCondition = conditionsList.stream().max(Comparator.comparingDouble(Condition::getPriority)).get();
        return maxPriorityCondition;
    }

    public void setConditionsList(ArrayList<Map<String, Object>> list) {
        list.forEach(action -> conditionsList.add(new Condition((String) action.get("field"),
                action.get("action").toString(),
                action.get("value").toString(),
                action.get("topic").toString(),
                (double) action.get("priority"))));
    }

    public Map<String, Object> getJsonMap() {
        return jsonMap;
    }

    public ArrayList<Condition> getConditionsList() {
        return conditionsList;
    }

    public void convertJsonToEntity(String content) throws IOException {
        try {
            jsonMap = gson.fromJson(appendToLog(content).toString(), Map.class);
            convertMapToJson();
        } catch (Exception e) {
            System.out.println("im ded" + e);
        }
    }

    private void jsonKeysToLowerCase(JsonObject jsonObject) {
        handleJsonObject(jsonObject);
    }

    private void handleValue(Object value) {
        if (value instanceof JsonObject) {
            handleJsonObject((JsonObject) value);
        } else if (value instanceof JsonArray) {
            handleJsonArray((JsonArray) value);
        } else {
            System.out.println("Value: {0}" + value);
        }
    }

    private void handleJsonArray(JsonArray jsonArray) {
        jsonArray.iterator().forEachRemaining(element -> {
            handleValue(element);
        });
    }

    private void handleJsonObject(JsonObject jsonObject) {
        jsonObject.keySet().forEach(key -> {
            Object value = jsonObject.get(key);
            key.toLowerCase();
            System.out.println("Key: {0}" + key);
            handleValue(value);
        });
    }

    private JsonObject appendToLog(String content) throws MalformedJsonException, JsonProcessingException {
        JsonObject obj = jParser.parse(content).getAsJsonObject();
        JsonObject logItem = new JsonObject();
        JsonArray arr = new JsonArray();

        logItem.add("moduleName", jParser.parse(this.moduleName));
        logItem.add("timeStamp", jParser.parse(gson.toJson(date.toString(), String.class)));

        if (obj.get("log") != null) {
            arr = obj.getAsJsonArray("log");
            arr.add(logItem);
        } else {
            arr.add(logItem);
            obj.add("log", arr);

        }
        jsonKeysToLowerCase(obj);
        return obj;
    }

    private void convertMapToJson() {
        java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
        }.getType();
        jsonObjectString = gson.toJson(jsonMap, gsonType);
    }

    public void director() {
        ArrayList<Map<String, Object>> list = (ArrayList<Map<String, Object>>) jsonMap.get("conditions");
        setConditionsList(list);
        removeCurrentLocationCondition();
        sendToKafkaQue(getHighestPriorityCondition());
    }

    private void removeCurrentLocationCondition() {
        for (int i = 0; i < conditionsList.size(); i++) {
            if (conditionsList.get(i).getTopic().equals(moduleName)) {
                conditionsList.remove(i);
                break;
            }
        }
    }

    private void sendToKafkaQue(Condition condition) {
        String routingSlipTopic = condition.getTopic();

        Properties config = new Properties();
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(config);
        producer.send(new ProducerRecord<String, String>(routingSlipTopic, jsonObjectString), new Callback() {
            @Override
            public void onCompletion(RecordMetadata rm, Exception excptn) {
                if (excptn != null) {
                    System.out.println("-------------onFailedQue------------------");
                }
            }
        });
        producer.close();
    }
}