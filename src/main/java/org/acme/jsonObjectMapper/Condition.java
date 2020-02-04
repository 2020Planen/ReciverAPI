package org.acme.jsonObjectMapper;

/**
 *
 * @author Magnus
 */
public class Condition {
    private String field,action,value,topic;
    double priority;

    public Condition(String field, String action, String value, String topic, double priority) {
        this.field = field;
        this.action = action;
        this.value = value;
        this.topic = topic;
        this.priority = priority;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public double getPriority() {
        return priority;
    }

    public void setPriority(double priority) {
        this.priority = priority;
    }

    

}
