package org.acme.jsonObjectMapper;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
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
    private RoutingSlip routingSlip;
    private Long exitTime, entryTime;
    private String idDB;
    private String producerReference;
    private MetaData metaData;
    private ErrorLog errorLog;
    private transient Date date = new Date();
    private transient Log currentLog;
    private transient Gson gson = new Gson();
    private transient Properties config = new Properties();

    public Message() {
    }

    public RoutingSlip getRoutingSlip() {
        return routingSlip;
    }

    public void setRoutingSlip(RoutingSlip routingSlip) {
        this.routingSlip = routingSlip;
    }

    /**
     * Starts the log with the name of the currently used module. Set the
     * current date, and the unix time.
     *
     * @param moduleName The name of the currently used module
     */
    public void startLog(String moduleName) {
        currentLog = new Log(moduleName, date.toString(), System.currentTimeMillis(), null, null);
    }

    /**
     * Sets the end unix time. And calls upon the method that sets the total
     * time spend in module. Adding log to the log ArrayList.
     */
    public void endLog() {
        currentLog.setEndTime(System.currentTimeMillis());
        currentLog.setTotalTime();
        logs.add(currentLog);
    }

    public Long getExitTime() {
        return exitTime;
    }

    public void setExitTime(Long exitTime) {
        this.exitTime = exitTime;
    }

    public Long getEntryTime() {
        return entryTime;
    }

    public void setEntryTime(Long entryTime) {
        this.entryTime = entryTime;
    }

    public String getIdDB() {
        return idDB;
    }

    public void setIdDB(String idDB) {
        this.idDB = idDB;
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

    /**
     * Calls routingSlip validation, find the highest priority route, with true
     * conditions. Check any valid route was returned. Opens a producer, that
     * will be given the routes topic.
     *
     * @throws Exception
     */
    public void sendToKafkaQue() throws Exception {
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Route route = routingSlip.validateRoutingSlip(metaData);
        if (route != null) {
            Producer<String, String> producer = new KafkaProducer<String, String>(config);
            producer.send(new ProducerRecord<String, String>(route.getTopic(), gson.toJson(this)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata rm, Exception excptn) {
                    if (excptn != null) {
                        System.out.println("-------------onFailedQue------------------");
                    }
                }
            });
            producer.close();
        } else {
            sendToKafkaExitQue();
        }
    }

    private void sendToKafkaErrorQue() {
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

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

    private void sendToKafkaNoValidProducerReferenceQue() {
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(config);
        producer.send(new ProducerRecord<String, String>("no-valid-producer-reference", gson.toJson(this)), new Callback() {
            @Override
            public void onCompletion(RecordMetadata rm, Exception excptn) {
                if (excptn != null) {
                    System.out.println("-------------onFailedErrorQue------------------");
                }
            }
        });
        producer.close();
    }

    /**
     * Send the Json directly to the end que.
     */
    public void sendToKafkaExitQue() {
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(config);
        producer.send(new ProducerRecord<String, String>("exit", gson.toJson(this)), new Callback() {
            @Override
            public void onCompletion(RecordMetadata rm, Exception excptn) {
                if (excptn != null) {
                    System.out.println("-------------onFailedErrorQue------------------");
                }
            }
        });
        producer.close();
    }

    /**
     * Add the thrown exception, add it to the errorLog for analyzation. Calls
     * upon kafka que handling.
     *
     * @param e Takes any exception
     */
    public void handleError(Exception e) {
        errorLog.setStackTrace(e);
        sendToKafkaErrorQue();
    }

}
