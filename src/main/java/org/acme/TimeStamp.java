package org.acme;

import java.util.Date;
import java.sql.Timestamp;

/**
 *
 * @author Magnus
 */
public class TimeStamp {

    private String moduleName;
    Date date = new Date();
    long time = date.getTime();
    Timestamp ts = new Timestamp(time);

    public TimeStamp(String moduleName) {
        this.moduleName = moduleName;
    }

    @Override
    public String toString() {
        return moduleName + " " + ts;
    }
}
