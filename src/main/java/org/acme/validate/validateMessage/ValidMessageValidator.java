package org.acme.validate.validateMessage;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.acme.jsonObjectMapper.Message;

/**
 *
 * @author Magnus
 */
public class ValidMessageValidator implements ConstraintValidator<ValidMessage, Message> {

    private ValidMessage annotation;

    /**
     *
     * @param constraintAnnotation
     */
    @Override
    public void initialize(ValidMessage constraintAnnotation) {
        this.annotation = constraintAnnotation;
    }

    /**
     *
     * @param message
     * @param context
     * @return
     */
    @Override
    public boolean isValid(Message message, ConstraintValidatorContext context) {
        String nullObject = "";
        boolean isValid = true;
        if (!(message instanceof Message)) {
            throw new IllegalArgumentException("@Message only applies to Message");
        }
        if (message.getMetaData() == null) {
            isValid = false;
            nullObject = "MetaData";
        }
        if (message.getData() == null) {
            isValid = false;
            nullObject = "Data";
        }
        if (!isValid) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate("Dosn't contain json object " + nullObject).addConstraintViolation();
        }
        return isValid;
    }

}
