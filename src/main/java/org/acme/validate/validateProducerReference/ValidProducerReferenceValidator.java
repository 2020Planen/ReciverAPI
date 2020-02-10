package org.acme.validate.validateProducerReference;

import org.acme.validate.*;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.acme.jsonObjectMapper.Message;

/**
 *
 * @author Magnus
 */
public class ValidProducerReferenceValidator implements ConstraintValidator<ValidProducerReference, Object> {

    private ValidProducerReference annotation;

    @Override
    public void initialize(ValidProducerReference constraintAnnotation) {
        this.annotation = constraintAnnotation;
    }

    @Override
    public boolean isValid(Object value, ConstraintValidatorContext context) {
        boolean isValid = true;
        System.out.println("\n\n\n\n\n\n");
        if (value instanceof Exception) {
            isValid = false;
        }
        if (value == null) {
            isValid = false;
        }

        return isValid;
    }

}
