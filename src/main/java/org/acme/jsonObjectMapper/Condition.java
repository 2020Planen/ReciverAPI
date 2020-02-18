package org.acme.jsonObjectMapper;

/**
 *
 * @author Magnus
 */
public class Condition {

    private String field, action;
    private Object value;

    /**
     *
     */
    public Condition() {
    }

    /**
     * All condition property must be set, for the validation process to be
     * valid.
     *
     * @param field String of the class.foo that the reflection need to invoke.
     * Example(MetaData.getAddress)
     * @param action The foo for which the value and field should be validate
     * on. Example (equals)
     * @param value The value the user wants to validate on. Example
     * (Nørrebrogade 155)
     */
    public Condition(String field, String action, Object value) {
        this.field = field;
        this.action = action;
        this.value = value;
    }

    /**
     * @return Class & function (MetaData.getAddress) for the given information
     * to analyze
     */
    public String getField() {
        return field;
    }

    /**
     * Adjustment to which metaData property is to be selected.
     *
     * @param field Take the class & function as a String(MetaData.getAddress)
     */
    public void setField(String field) {
        this.field = field;
    }

    /**
     * @return Which action is need to for validation as String.
     */
    public String getAction() {
        return action;
    }

    /**
     * Adjustment to which foo to validate on the given value of field property.
     *
     * @param action Setting action is need to for validation as String.
     */
    public void setAction(String action) {
        this.action = action;
    }

    /**
     * @return The object which should be validate on.
     */
    public Object getValue() {
        return value;
    }

    /**
     * Setting the value to be validated on.
     *
     * @param value Sets any kind of object that needs to be validate
     */
    public void setValue(Object value) {
        this.value = value;
    }

}
