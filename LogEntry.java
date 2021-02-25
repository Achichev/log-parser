package logs_parser;

import java.util.Date;
/*
 * The class contains each log parameters
 */
public class LogEntry {
    private String ip;
    private String name;
    private Date date;
    private Event event;
    private int taskNumber;
    private Status status;

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public void setEvent(Event event) {
        this.event = event;
    }

    public void setTaskNumber(int taskNumber) {
        this.taskNumber = taskNumber;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getIp() {
        return ip;
    }

    public String getName() {
        return name;
    }

    public Date getDate() {
        return date;
    }

    public Event getEvent() {
        return event;
    }

    public int getTaskNumber() {
        return taskNumber;
    }

    public Status getStatus() {
        return status;
    }

}
