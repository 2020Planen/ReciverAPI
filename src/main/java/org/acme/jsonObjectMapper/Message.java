package org.acme.jsonObjectMapper;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
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
public class Message {

    private Data data;
    private ArrayList<Log> logs = new ArrayList<>();
    private ArrayList<Condition> conditionsList = new ArrayList<>();
    private ArrayList<Condition> usedConditionsList = new ArrayList<>();
    private String idDB;
    private String producerReference;
    private MetaData metaData;
    private ErrorLog errorLog;
    private transient Date date = new Date();
    private transient Log currentLog;
    private transient Gson gson = new Gson();

    public Message() {
        this.metaData = new MetaData();
        this.errorLog = new ErrorLog();
    }

    public void startLog(String moduleName) {
        currentLog = new Log(moduleName, date.toString(), System.currentTimeMillis(), null, null);
    }

    public void endLog() {
        System.out.println("---------------------------------- " + System.currentTimeMillis());
        currentLog.setEndTime(System.currentTimeMillis());
        currentLog.setTotalTime();

        logs.add(currentLog);
    }

    public ArrayList<Condition> getUsedConditionsList() {
        return usedConditionsList;
    }

    public void setUsedConditionsList(ArrayList<Condition> usedConditionsList) {
        this.usedConditionsList = usedConditionsList;
    }

    public String getIdDB() {
        return idDB;
    }

    public void setIdDB(String idDB) {
        this.idDB = idDB;
    }

    public ArrayList<Condition> getConditionsList() {
        return conditionsList;
    }

    public Data getData() {
        return data;
    }

    public void setData(Data data) {
        this.data = data;
    }

    public String getProducerReference() {
        return producerReference;
    }

    public void setProducerReference(String producerReference) {
        this.producerReference = producerReference;
    }

    public ArrayList<Log> getLogs() {
        return logs;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    public void addLogs(JsonArray logInJsonFormat) {
        logInJsonFormat.forEach(log -> logs.add(new Log(
                log.getAsJsonObject().get("moduleName").getAsString(),
                log.getAsJsonObject().get("timeStamp").getAsString(),
                log.getAsJsonObject().get("timeInModule").getAsJsonObject().get("startTimeMs").getAsLong(),
                log.getAsJsonObject().get("timeInModule").getAsJsonObject().get("endTime").getAsLong(),
                log.getAsJsonObject().get("timeInModule").getAsJsonObject().get("totalTime").getAsLong()
        )));
    }

    public void validation(String key, Object value) {
        System.out.println("\n________________value" + value + "________________\n");

        if (key.equals("address")) {
            this.metaData.setAddress((String) value);
        }
        if (key.equals("name")) {
            metaData.setName((String) value);
        }
        if (key.equals("city")) {
            metaData.setCity((String) value);
        }
        if (key.equals("phone")) {
            metaData.setPhone((String) value);
        }
        if (key.equals("zip")) {
            metaData.setZip((Integer) value);
        }

    }

    public void sendToKafkaQue() {
        Properties config = new Properties();
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        endLog();
        Condition condition = getHighestPriorityCondition();
        Producer<String, String> producer = new KafkaProducer<String, String>(config);
        producer.send(new ProducerRecord<String, String>(condition.getTopic(), gson.toJson(this)), new Callback() {
            @Override
            public void onCompletion(RecordMetadata rm, Exception excptn) {
                if (excptn != null) {
                    System.out.println("-------------onFailedQue------------------");
                }
            }
        });
        producer.close();
        removeHighestPriorityCondition(condition);
    }

    private void sendToKafkaErrorQue() {
        Properties config = new Properties();
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        endLog();

        Producer<String, String> producer = new KafkaProducer<String, String>(config);
        producer.send(new ProducerRecord<String, String>("error", gson.toJson(this)), new Callback() {
            @Override
            public void onCompletion(RecordMetadata rm, Exception excptn) {
                if (excptn != null) {
                    System.out.println("-------------onFailedErrorQue------------------");
                }
            }
        });
        producer.close();
    }

    public void handleError(Exception e) {
        errorLog.setStackTrace(e);
        sendToKafkaErrorQue();
    }

    private Condition getHighestPriorityCondition() {
        if (getConditionsList().size() == 1) {
            return getConditionsList().get(0);
        } else {
            return getConditionsList().stream().max(Comparator.comparingDouble(Condition::getPriority)).get();
        }
    }

    private void removeHighestPriorityCondition(Condition condition) {
        for (int i = 0; i < getConditionsList().size(); i++) {
            if (getConditionsList().get(i).getTopic().equals(condition.getTopic())) {
                getUsedConditionsList().add(getConditionsList().get(i));
                getConditionsList().remove(i);
            }
        }
    }
}
