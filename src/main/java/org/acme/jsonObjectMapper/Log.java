package org.acme.jsonObjectMapper;

import java.util.Map;

/**
 *
 * @author Magnus
 */
public class Log {

    private String moduleName, timeStamp;
    private Long startTime, endTime, totalTime;

    public Log(String moduleName, String timeStamp, Long startTime, Long endTime, Long totalTime) {
        this.moduleName = moduleName;
        this.timeStamp = timeStamp;
        this.startTime = startTime;
        this.endTime = endTime;
        this.totalTime = totalTime;
    }
    
    public String getModuleName() {
        return moduleName;
    }

    public void setModuleName(String moduleName) {
        this.moduleName = moduleName;
    }

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public Long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime() {
        this.totalTime = this.endTime-this.startTime;
    }

}
