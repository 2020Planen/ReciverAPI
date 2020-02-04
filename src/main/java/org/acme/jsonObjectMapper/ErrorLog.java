package org.acme.jsonObjectMapper;

/**
 *
 * @author Magnus
 */
public class ErrorLog {
private String stackTrace;


    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(Exception e) {
        this.stackTrace = e.getStackTrace().toString();
    }

}
