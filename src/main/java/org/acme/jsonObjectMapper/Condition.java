package org.acme.jsonObjectMapper;

import java.util.ArrayList;

/**
 *
 * @author Magnus
 */
public class Condition {

    private String field, action, topic;
    private Object value;
    double priority;
    private boolean lastCondi;
    private ArrayList<Condition> conditions = new ArrayList<>();

    public Condition() {
    }

    public Condition(String field, String action, String topic, Object value, double priority, boolean lastCondi) {
        this.field = field;
        this.action = action;
        this.topic = topic;
        this.value = value;
        this.priority = priority;
        this.lastCondi = lastCondi;
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

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public double getPriority() {
        return priority;
    }

    public void setPriority(double priority) {
        this.priority = priority;
    }

    public boolean isLastCondi() {
        return lastCondi;
    }

    public void setLastCondi(boolean lastCondi) {
        this.lastCondi = lastCondi;
    }

    public ArrayList<Condition> getConditions() {
        return conditions;
    }

    public void setConditions(ArrayList<Condition> conditions) {
        this.conditions = conditions;
    }
    
    

}
