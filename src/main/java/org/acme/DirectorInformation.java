package org.acme;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 *
 * @author Magnus
 */
public class DirectorInformation {

    Gson gson = new Gson();
    private ArrayList<Condition> conditionsList = new ArrayList<>();
    private Map<String, Object> jsonMap;
    private final String moduleName;
    Date date = new Date();
    long time = date.getTime();
    Timestamp ts = new Timestamp(time);

    public DirectorInformation(String moduleName) {
        this.moduleName = moduleName;
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

    public void convertJsonToEntity(String content) {
        this.jsonMap = gson.fromJson(content, Map.class);
        addTimestamp();
    }

    public String convertMapToJson(Map map) {
        java.lang.reflect.Type gsonType = new TypeToken<HashMap>() {
        }.getType();
        return gson.toJson(map, gsonType);
    }

    public void director() {
        ArrayList<Map<String, Object>> list = (ArrayList<Map<String, Object>>) jsonMap.get("conditions");
        setConditionsList(list);
        removeCurrentLocationCondition();
        sendToKafkaQue(getHighestPriorityCondition());
    }

    private void addTimestamp() {
        this.jsonMap.put("timestamp", new String[]{this.moduleName + " " + ts.toString()});
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
        String jsonToOutput = convertMapToJson(jsonMap);

        Properties config = new Properties();
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        //config.put("group.id", "DirectorInformation");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(config);
        producer.send(new ProducerRecord<String, String>(routingSlipTopic, jsonToOutput), new Callback() {
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
