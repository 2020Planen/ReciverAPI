/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.acme.jsonObjectMapper;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Magnus
 */
public class Route {

    private String topic;
    double priority;
    private List<Condition> conditions = new ArrayList<>();
    private List<Route> routes = new ArrayList<>();

    /**
     *
     */
    public Route() {
    }

    /**
     * The name of the next module the json object should be send to.
     *
     * @return The topic which the kafka producer should send to.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Adjust what the next module in the route is.
     *
     * @param topic String name of the next module.
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * The priority for which the routes are chosen.
     *
     * @return The priority in double.
     */
    public double getPriority() {
        return priority;
    }

    /**
     * Adjusting what the priority of the module are chosen.
     *
     * @param priority Priority should always be higher than 0.0, with no
     * double_max.
     */
    public void setPriority(double priority) {
        this.priority = priority;
    }

    /**
     *
     * @return
     */
    public List<Condition> getConditions() {
        return conditions;
    }

    /**
     *
     * @param conditions
     */
    public void setConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }

    /**
     *
     * @return
     */
    public List<Route> getRoutes() {
        return routes;
    }

    /**
     *
     * @param routes
     */
    public void setRoutes(List<Route> routes) {
        this.routes = routes;
    }

    /**
     * Loops through the routes conditions list, and evaluate each condition for
     * each route.
     *
     * @param metaData The MetaData received from the message
     * @return False if the given conditions contains a false argument, else
     * return true can be used as route
     * @throws Exception TODO
     */
    public boolean validateConditionList(MetaData metaData) throws Exception {
        for (Condition condition : this.conditions) {
            if (!validateCondition(condition, metaData)) {
                return false;
            }
        }
        return true;
    }

    private boolean validateCondition(Condition condition, MetaData metaData) throws Exception {
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
            Method method = clsString.getDeclaredMethod(condition.getAction(), parameterType);

            boolean condtionValidationState = (boolean) method.invoke(valueOfField, condition.getValue());
            return condtionValidationState;

        } else if (valueParameterType == BigDecimal.class
                || valueParameterType == Integer.class
                || valueParameterType == Long.class
                || valueParameterType == Float.class) {
            BigDecimal bd = (BigDecimal) condition.getValue();
            double valueDouble = bd.doubleValue();
            double fieldDouble = Double.parseDouble(valueOfField.toString());

            Class clsAction = Class.forName("org.acme.jsonObjectMapper.Message");
            Method methodAction = clsAction.getDeclaredMethod(condition.getAction(), double.class,
                    double.class
            );

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

}
