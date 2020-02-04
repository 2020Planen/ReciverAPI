package org.acme.jsonObjectMapper;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author Magnus
 */
public class PlayGroundTest {

    private ArrayList<Condition> conditionsList = new ArrayList<>();
    private final String moduleName;
    private JsonObject jsonMessage;
    private ArrayList<String> noRemovalModules = new ArrayList<>();
    private Long startTime, endTime, totalTime;

    private Date date = new Date();
    private Gson gson = new Gson();
    private JsonParser jsonParser = new JsonParser();

    public PlayGroundTest(String moduleName, String message) {
        this.startTime = System.currentTimeMillis();
        this.moduleName = moduleName;
        this.jsonMessage = (JsonObject) jsonParser.parse(message);
        noRemovalModules.add("routing");
        System.out.println(jsonMessage.getAsJsonArray("log").size() + "--------------------");

    }



    public JsonObject getJsonMessage() {
        return jsonMessage;
    }

    public void printOutJson() {
        System.out.println("\n" + jsonMessage + "\n");
        System.out.println(conditionsList.size());
    }

    public void appendJson(String key, Object value) {
        if (jsonMessage.get(key) != null && jsonMessage.get(key) instanceof JsonArray) {
            appendJsonArray(key, value);
        } else {
            jsonMessage.add(key, jsonParser.parse(value.toString()));
        }
    }

    private void appendJsonArray(String key, Object value) {
        JsonArray newJsonArray = new JsonArray();
        newJsonArray = jsonMessage.getAsJsonArray(key);
        newJsonArray.add(jsonParser.parse(value.toString()));
    }

    public void routingSlipDirector() {
        if (jsonMessage.getAsJsonArray("conditions") != null) {
            JsonArray conditions = jsonMessage.getAsJsonArray("conditions");
            setConditions(conditions);
            conditions = removeHighestPriorityCondition(conditions, getHighestPriorityCondition());
            sendToKafkaQue(getHighestPriorityCondition());
        } else {
            timeInModule();
        }
    }

    public JsonArray removeHighestPriorityCondition(JsonArray conditions, Condition highestPriorityCondition) {
        if (!noRemovalModules.contains(moduleName)) {
            for (int i = 0; i < conditions.size(); i++) {
                if (conditions.get(i).getAsJsonObject().get("topic").toString().equals(highestPriorityCondition.getTopic())) {
                    conditions.remove(i);
                    conditionsList.remove(i);
                    return conditions;
                }
            }
        }
        return conditions;
    }

    private void setConditions(JsonArray conditions) {
        conditions.forEach(condition -> conditionsList.add(new Condition(
                condition.getAsJsonObject().get("field").getAsString(),
                condition.getAsJsonObject().get("action").getAsString(),
                condition.getAsJsonObject().get("value").getAsString(),
                condition.getAsJsonObject().get("topic").getAsString(),
                Double.valueOf(condition.getAsJsonObject().get("priority").getAsString()))));
    }

    private Condition getHighestPriorityCondition() {
        return conditionsList.stream().max(Comparator.comparingDouble(Condition::getPriority)).get();
    }

    public String getJsonMessageString() {
        return jsonMessage.getAsString();
    }

    private void appendToLog(JsonObject timeObject) {
        JsonObject logItem = new JsonObject();
        JsonArray logArray = new JsonArray();

        logItem.addProperty("moduleName", moduleName);
        logItem.addProperty("timeStamp", date.toString());
        logItem.add("timeInModule", timeObject);

        if (jsonMessage.get("log") != null) {
            logArray = jsonMessage.getAsJsonArray("log");
            logArray.add(logItem);
        } else {
            logArray.add(logItem);
            jsonMessage.add("log", logArray);
        }
    }

    public void handleError(String exception) {
        jsonMessage.addProperty("errorLog", exception);
        sendToKafkaErrorQue();
    }

    private void sendToKafkaQue(Condition condition) {
        String routingSlipTopic = condition.getTopic().replace("\"", "");
        Properties config = new Properties();
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        timeInModule();

        Producer<String, String> producer = new KafkaProducer<String, String>(config);
        producer.send(new ProducerRecord<String, String>(routingSlipTopic, jsonMessage.getAsString()), new Callback() {
            @Override
            public void onCompletion(RecordMetadata rm, Exception excptn) {
                if (excptn != null) {
                    System.out.println("-------------onFailedQue------------------");
                }
            }
        });
        producer.close();
    }

    private void sendToKafkaErrorQue() {
        Properties config = new Properties();
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        timeInModule();

        Producer<String, String> producer = new KafkaProducer<String, String>(config);
        producer.send(new ProducerRecord<String, String>("error", getJsonMessageString()), new Callback() {
            @Override
            public void onCompletion(RecordMetadata rm, Exception excptn) {
                if (excptn != null) {
                    System.out.println("-------------onFailedQue------------------");
                }
            }
        });
        producer.close();
    }

    private void timeInModule() throws JsonSyntaxException {
        this.endTime = System.currentTimeMillis();
        totalTime = endTime - startTime;
        JsonObject timeObject = new JsonObject();
        timeObject.addProperty("startTimeMs", startTime);
        timeObject.addProperty("endtimeMs", endTime);
        timeObject.addProperty("totalTimeMs", totalTime);
        appendToLog(timeObject);
    }
}
