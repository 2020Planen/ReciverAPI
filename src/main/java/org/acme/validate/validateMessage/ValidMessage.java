package org.acme.validate.validateMessage;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE_USE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.validation.Constraint;
import javax.validation.Payload;

/**
 *
 * @author Magnus
 */

@Target({FIELD, PARAMETER, ANNOTATION_TYPE, TYPE_USE})
@Retention(RUNTIME)
@Constraint(validatedBy = ValidMessageValidator.class)
@Documented
public @interface ValidMessage {

    String moduleName();
    
    /**
     *
     * @return Default message if no specific custom message has been set
     */
    String message() default "Must not be null";

    /**
     *
     * @return Nothing if if no specific custom Class's has been set
     */
    Class<?>[] groups() default {};

    /**
     *
     * @return Nothing if if no specific custom Class's has been set
     */
    Class<? extends Payload>[] payload() default {};

}

