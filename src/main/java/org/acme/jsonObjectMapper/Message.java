package org.acme.jsonObjectMapper;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
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

    public void startLog(String moduleName) {
        currentLog = new Log(moduleName, date.toString(), System.currentTimeMillis(), null, null);
    }

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

    public ArrayList<Condition> getConditionsList() {
        return conditionsList;
    }

    public void setConditionsList(ArrayList<Condition> conditionsList) {
        this.conditionsList = conditionsList;
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

    public void sendToKafkaQue() throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
        config.put("bootstrap.servers", "cis-x.convergens.dk:9092");
        config.put("retries", 0);
        config.put("acks", "all");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Condition condition = validateCondtionSlip();
        updateConditionList(condition);
        if (conditionsList.isEmpty() || condition == null) {
            sendToKafkaExitQue();
        } else {
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

    public void sendToKafkaNoValidProducerReferenceQue() {
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

    public void handleError(Exception e) {
        errorLog.setStackTrace(e);
        sendToKafkaErrorQue();
    }

    private void updateConditionList(Condition condition) {
        try {
            conditionsList = condition.getConditions();
        } catch (NullPointerException e) {
            System.out.println("In catch_________________");
            conditionsList = new ArrayList<Condition>();
        }
    }

    public Condition validateCondtionSlip() throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
        conditionsList.sort(Comparator.comparing(Condition::getPriority).reversed());
        for (Condition condition : conditionsList) {
            if (validateCondition(condition)) {
                System.out.println("finds________________________________________________________________");
                return condition;
            }
        }
        sendToKafkaExitQue();
        return null;
    }

    private boolean validateCondition(Condition condition) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, IllegalArgumentException, InvocationTargetException {
        Class noparams[] = {};
        Class clsField = Class.forName("org.acme.jsonObjectMapper." + condition.getField().substring(0, condition.getField().indexOf(".")));
        Method methodField = clsField.getDeclaredMethod(condition.getField().substring(condition.getField().indexOf(".") + 1), noparams);
        Object valueOfField = methodField.invoke(metaData);
        Type valueParameterType = condition.getValue().getClass();

        if (valueParameterType == String.class) {
            Class<?>[] parameterType = null;
            Class clsString = Class.forName("java.lang.String");
            Method[] allMethods = clsString.getDeclaredMethods();
            for (Method m : allMethods) {
                if (!m.getName().equals(condition.getAction())) {
                    continue;
                }
                parameterType = m.getParameterTypes();
            }
            System.out.println(parameterType);
            Method method = clsString.getDeclaredMethod(condition.getAction(), parameterType);

            boolean condtionValidationState = (boolean) method.invoke(valueOfField, condition.getValue());
            return condtionValidationState;
        } else if (valueParameterType == BigDecimal.class || valueParameterType == Integer.class || valueParameterType == Long.class || valueParameterType == Float.class) {
            BigDecimal bd = (BigDecimal) condition.getValue();
            double valueDouble = bd.doubleValue();
            double fieldDouble = Double.parseDouble(valueOfField.toString());

            Class clsAction = Class.forName("org.acme.jsonObjectMapper.Message");
            Method methodAction = clsAction.getDeclaredMethod(condition.getAction(), double.class, double.class);

            boolean condtionValidationState = (boolean) methodAction.invoke(this, fieldDouble, valueDouble);
            return condtionValidationState;
        }
        return false;
    }

    private boolean greaterThan(double valueOfField, double valueOfValue) {
        return valueOfField > valueOfValue;
    }

    private boolean lessThan(double valueOfField, double valueOfValue) {
        return valueOfField < valueOfValue;
    }

    private boolean equal(double valueOfField, double valueOfValue) {
        return valueOfField == valueOfValue;
    }

    @Override
    public String toString() {
        return "Message{" + "data=" + data + ", logs=" + logs + ", conditionsList=" + conditionsList + ", idDB=" + idDB + ", producerReference=" + producerReference + ", metaData=" + metaData + ", errorLog=" + errorLog + ", date=" + date + ", currentLog=" + currentLog + ", gson=" + gson + '}';
    }

}
