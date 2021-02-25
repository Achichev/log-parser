package logs_parser;

import logs_parser.query.*;
import org.apache.commons.collections4.list.TreeList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
/*
 * The class parses *.log files from logDir directory
 */
public class LogParser implements IPQuery, UserQuery, DateQuery, EventQuery, QLQuery {
    private Path logDir;
    public LogParser(Path logDir) {
        this.logDir = logDir;
    }
    private SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

    //the method returns a list of all log entries enclosed between specified dates inclusive
    //from all files in logDir directory
    public List<LogEntry> getEntriesByDate(Date after, Date before) {
        List<LogEntry> entriesByDate = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(logDir)) {

                for (Path entry : stream) {
                    if (entry.getFileName().toString().endsWith(".log")) {
                        try (BufferedReader in = new BufferedReader(new FileReader(entry.toString()))) {
                            while (in.ready()) {
                                String[] params = in.readLine().split("\t");
                                LogEntry logEntry = new LogEntry();
                                logEntry.setIp(params[0]);
                                logEntry.setName(params[1]);
                                logEntry.setDate(sdf.parse(params[2]));

                                if (params[3].contains(" ")
                                        && (params[3].contains("SOLVE_TASK") || params[3].contains("DONE_TASK"))) {
                                    String[] tempArr = params[3].split(" ");
                                    logEntry.setEvent(Event.valueOf(tempArr[0]));
                                    logEntry.setTaskNumber(Integer.parseInt(tempArr[1]));
                                } else {
                                    logEntry.setEvent(Event.valueOf(params[3]));
                                }
                                logEntry.setStatus(Status.valueOf(params[4]));
                                entriesByDate.add(logEntry);
                            }
                        }
                    }
                }
            } catch (IOException | ParseException ignored) {}

        entriesByDate.removeIf(logEntry -> !((after == null || logEntry.getDate().after(after))
                && (before == null || logEntry.getDate().before(before))));

        return entriesByDate;
    }

    //returns the amount of all unique IPs from all log entries enclosed between specified dates inclusive
    @Override
    public int getNumberOfUniqueIPs(Date after, Date before) {
        return getUniqueIPs(after, before).size();
    }

    //returns a set of all unique IPs from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getUniqueIPs(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .map(LogEntry::getIp)
                .collect(Collectors.toSet());
    }

    //returns a set of all unique IPs that belonged to the specified user
    //from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getIPsForUser(String user, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getName().equals(user))
                .map(LogEntry::getIp)
                .collect(Collectors.toSet());
    }

    //returns a set of all unique IPs that belonged to the log with the specified event
    //from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getIPsForEvent(Event event, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getEvent().equals(event))
                .map(LogEntry::getIp)
                .collect(Collectors.toSet());
    }

    //returns a set of all unique IPs which belong to the log with the specified status
    //from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getIPsForStatus(Status status, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getStatus().equals(status))
                .map(LogEntry::getIp)
                .collect(Collectors.toSet());
    }

    //returns a set of all usernames from all *.log files
    @Override
    public Set<String> getAllUsers() {
        return getEntriesByDate(null, null)
                .stream()
                .map(LogEntry::getName)
                .collect(Collectors.toSet());
    }

    //returns the amount of all unique usernames from all log entries enclosed between specified dates inclusive
    @Override
    public int getNumberOfUsers(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .map(LogEntry::getName)
                .collect(Collectors.toSet()).size();
    }

    //returns the amount of all unique events which belong to the specified user
    //from all log entries enclosed between specified dates inclusive
    @Override
    public int getNumberOfUserEvents(String user, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getName().equals(user))
                .map(LogEntry::getEvent)
                .collect(Collectors.toSet())
                .size();
    }

    //returns set of all unique user names with the specified ip
    //from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getUsersForIP(String ip, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getIp().equals(ip))
                .map(LogEntry::getName)
                .collect(Collectors.toSet());
    }

    //returns set of unique user names with the Event value of "LOGIN"
    // from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getLoggedUsers(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getEvent().equals(Event.LOGIN))
                .map(LogEntry::getName)
                .collect(Collectors.toSet());
    }

    //returns a set of unique user names with the Event value of "DOWNLOAD_PLUGIN"
    //from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getDownloadedPluginUsers(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getEvent().equals(Event.DOWNLOAD_PLUGIN))
                .map(LogEntry::getName)
                .collect(Collectors.toSet());
    }

    //returns a set of unique user names with the Event value of "WRITE_MESSAGE"
    //from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getWroteMessageUsers(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getEvent().equals(Event.WRITE_MESSAGE))
                .map(LogEntry::getName)
                .collect(Collectors.toSet());
    }

    //returns a set of unique user names with the Event value of "WRITE_MESSAGE"
    //from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getEvent().equals(Event.SOLVE_TASK))
                .map(LogEntry::getName)
                .collect(Collectors.toSet());
    }

    //returns a set of unique user names with the Event value of "SOLVE_TASK" and the taskNumber
    //equal to specified task from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getSolvedTaskUsers(Date after, Date before, int task) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getTaskNumber() != 0 && e.getTaskNumber() == task && e.getEvent().equals(Event.SOLVE_TASK))
                .map(LogEntry::getName)
                .collect(Collectors.toSet());
    }

    //returns a set of unique user names with the Event value of "DONE_TASK"
    //from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getEvent().equals(Event.DONE_TASK))
                .map(LogEntry::getName)
                .collect(Collectors.toSet());
    }

    //returns a set of unique user names with the Event value of "DONE_TASK" and the taskNumber
    //equal to specified task from all log entries enclosed between specified dates inclusive
    @Override
    public Set<String> getDoneTaskUsers(Date after, Date before, int task) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getTaskNumber() != 0 && e.getTaskNumber() == task && e.getEvent().equals(Event.DONE_TASK))
                .map(LogEntry::getName)
                .collect(Collectors.toSet());
    }

    //returns a set of unique dates for the specified period
    public Set<Date> getAllDates(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .map(LogEntry::getDate)
                .collect(Collectors.toSet());
    }

    //returns a set of unique dates when the user has made the event
    @Override
    public Set<Date> getDatesForUserAndEvent(String user, Event event, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getName().equals(user) && e.getEvent().equals(event))
                .map(LogEntry::getDate)
                .collect(Collectors.toSet());
    }

    //returns a set of unique dates which match to FAILED event
    @Override
    public Set<Date> getDatesWhenSomethingFailed(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getStatus().equals(Status.FAILED))
                .map(LogEntry::getDate)
                .collect(Collectors.toSet());
    }

    //returns a set of unique dates which match to ERROR event
    @Override
    public Set<Date> getDatesWhenErrorHappened(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getStatus().equals(Status.ERROR))
                .map(LogEntry::getDate)
                .collect(Collectors.toSet());
    }

    //returns the date when the user logged in first for the period or null
    @Override
    public Date getDateWhenUserLoggedFirstTime(String user, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .sorted(Comparator.comparing(LogEntry::getDate))
                .filter(e -> e.getName().equals(user) && e.getEvent().equals(Event.LOGIN))
                .findFirst()
                .map(LogEntry::getDate)
                .orElse(null);
    }

    //returns the date when the user tried to solve the task first for the period or null
    @Override
    public Date getDateWhenUserSolvedTask(String user, int task, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .sorted(Comparator.comparing(LogEntry::getDate))
                .filter(e -> e.getName().equals(user) && e.getEvent().equals(Event.SOLVE_TASK) && e.getTaskNumber() == task)
                .findFirst()
                .map(LogEntry::getDate)
                .orElse(null);
    }

    //returns the date when the user solved the task first for the period or null
    @Override
    public Date getDateWhenUserDoneTask(String user, int task, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .sorted(Comparator.comparing(LogEntry::getDate))
                .filter(e -> e.getName().equals(user) && e.getEvent().equals(Event.DONE_TASK) && e.getTaskNumber() == task)
                .findFirst()
                .map(LogEntry::getDate)
                .orElse(null);
    }

    //returns a set of unique dates when the user wrote a message for the period
    @Override
    public Set<Date> getDatesWhenUserWroteMessage(String user, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getName().equals(user) && e.getEvent().equals(Event.WRITE_MESSAGE))
                .map(LogEntry::getDate)
                .collect(Collectors.toSet());
    }

    //returns a set of unique dates when the user downloaded plugin for the period
    @Override
    public Set<Date> getDatesWhenUserDownloadedPlugin(String user, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getName().equals(user) && e.getEvent().equals(Event.DOWNLOAD_PLUGIN))
                .map(LogEntry::getDate)
                .collect(Collectors.toSet());
    }

    //returns the amount of unique events for the specified period
    @Override
    public int getNumberOfAllEvents(Date after, Date before) {
        return getAllEvents(after, before).size();
    }

    //returns a set of unique events for the specified period
    @Override
    public Set<Event> getAllEvents(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .map(LogEntry::getEvent)
                .collect(Collectors.toSet());
    }

    //returns a set of unique events from the specified IP for the period
    @Override
    public Set<Event> getEventsForIP(String ip, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getIp().equals(ip))
                .map(LogEntry::getEvent)
                .collect(Collectors.toSet());
    }

    //returns a set of unique events initiated by the specified user for the period
    @Override
    public Set<Event> getEventsForUser(String user, Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getName().equals(user))
                .map(LogEntry::getEvent)
                .collect(Collectors.toSet());
    }

    //returns a set of unique failed events for the period
    @Override
    public Set<Event> getFailedEvents(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getStatus().equals(Status.FAILED))
                .map(LogEntry::getEvent)
                .collect(Collectors.toSet());
    }

    //returns a set of unique error events for the period
    @Override
    public Set<Event> getErrorEvents(Date after, Date before) {
        return getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getStatus().equals(Status.ERROR))
                .map(LogEntry::getEvent)
                .collect(Collectors.toSet());
    }

    //returns the amount of attempts to solve the specified task for the period
    @Override
    public int getNumberOfAttemptToSolveTask(int task, Date after, Date before) {
        return (int) getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getTaskNumber() == task && e.getEvent().equals(Event.SOLVE_TASK))
                .count();
    }

    //returns the amount of attempts to get the specified task done for the period
    @Override
    public int getNumberOfSuccessfulAttemptToSolveTask(int task, Date after, Date before) {
        return (int) getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getTaskNumber() == task && e.getEvent().equals(Event.DONE_TASK))
                .count();
    }

    //returns a map of task numbers and amounts of attempts to solve each task for the period
    @Override
    public Map<Integer, Integer> getAllSolvedTasksAndTheirNumber(Date after, Date before) {
        Set<Integer> set = getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getEvent().equals(Event.SOLVE_TASK))
                .map(LogEntry::getTaskNumber)
                .collect(Collectors.toSet());

        return set.stream()
                .collect(Collectors.toMap(i -> i, i -> getNumberOfAttemptToSolveTask(i, after, before), (a, b) -> b));
    }

    //returns a map of task numbers and amounts of attempts to get each task done for the period
    @Override
    public Map<Integer, Integer> getAllDoneTasksAndTheirNumber(Date after, Date before) {
        Set<Integer> set = getEntriesByDate(after, before)
                .stream()
                .filter(e -> e.getEvent().equals(Event.DONE_TASK))
                .map(LogEntry::getTaskNumber)
                .collect(Collectors.toSet());
        return set.stream()
                .collect(Collectors.toMap(i -> i, i -> getNumberOfSuccessfulAttemptToSolveTask(i, after, before), (a, b) -> b));
    }

    //parses queries and returns a set of objects which were requested
    @Override
    public Set<Object> execute(String query) {
        Set<Object> set = null;
        if (query.startsWith("get") && !query.contains("=")) {
            switch (query) {
                case "get ip":
                    set = new HashSet<>(getUniqueIPs(null, null));
                    break;
                case "get user":
                    set = new HashSet<>(getAllUsers());
                    break;
                case "get date":
                    set = new HashSet<>(getAllDates(null, null));
                    break;
                case "get event":
                    set = new HashSet<>(getAllEvents(null, null));
                    break;
                case "get status":
                    set = getEntriesByDate(null, null).stream().map(LogEntry::getStatus)
                            .collect(Collectors.toSet());
                    break;
            }
        } else {

            String[] queryParts = query.split("=");
            String[] leftPartOfQuery = queryParts[0].split(" ");
            String field1 = leftPartOfQuery[1].trim();
            String field2 = leftPartOfQuery[3].trim();
            String value1 = queryParts[1].substring(queryParts[1].indexOf("\"") + 1, queryParts[1].indexOf("\"", queryParts[1].indexOf("\"")+1));
            Date after = null;
            Date before = null;

            if (queryParts[1].contains("and date between")) {
                String datesBetween = queryParts[1].split("and date between")[1].replace("\"", "").trim();
                String[] dates = datesBetween.split("and");
                try {
                    after = sdf.parse(dates[0].trim());
                    before = sdf.parse(dates[1].trim());
                } catch (ParseException ignored) {
                }
            }

            switch (field1) {
                case "ip":
                    switch (field2) {
                        case "user":
                            set = new HashSet<>(getIPsForUser(value1, after, before));
                            break;
                        case "date":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getDate().equals(readDate(value1)))
                            .map(LogEntry::getIp).collect(Collectors.toSet());
                            break;
                        case "event":
                            set = new HashSet<>(getIPsForEvent(readEvent(value1), after, before));
                            break;
                        case "status":
                            set = new HashSet<>(getIPsForStatus(readStatus(value1), after, before));
                            break;
                    }
                    break;
                case "user":
                    switch (field2) {
                        case "ip":
                            set = new HashSet<>(getUsersForIP(value1, after, before));
                            break;
                        case "date":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getDate().equals(readDate(value1)))
                                    .map(LogEntry::getName).collect(Collectors.toSet());
                            break;
                        case "event":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getEvent() == readEvent(value1))
                                    .map(LogEntry::getName).collect(Collectors.toSet());
                            break;
                        case "status":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getStatus().equals(readStatus(value1)))
                                    .map(LogEntry::getName).collect(Collectors.toSet());
                            break;
                    }
                    break;
                case "date":
                    switch (field2) {
                        case "ip":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getIp().equals(value1))
                                    .map(LogEntry::getDate).collect(Collectors.toSet());
                            break;
                        case "user":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getName().equals(value1))
                                    .map(LogEntry::getDate).collect(Collectors.toSet());
                            break;
                        case "event":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getEvent().equals(readEvent(value1)))
                                    .map(LogEntry::getDate).collect(Collectors.toSet());
                            break;
                        case "status":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getStatus().equals(readStatus(value1)))
                                    .map(LogEntry::getDate).collect(Collectors.toSet());
                            break;
                    }
                    break;
                case "event":
                    switch (field2) {
                        case "ip":
                            set = new HashSet<>(getEventsForIP(value1, after, before));
                            break;
                        case "user":
                            set = new HashSet<>(getEventsForUser(value1, after, before));
                            break;
                        case "date":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getDate().equals(readDate(value1)))
                            .map(LogEntry::getEvent).collect(Collectors.toSet());
                            break;
                        case "status":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getStatus().equals(readStatus(value1)))
                                    .map(LogEntry::getEvent).collect(Collectors.toSet());
                            break;
                    }
                    break;
                case "status":
                    switch (field2) {
                        case "ip":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getIp().equals(value1))
                                    .map(LogEntry::getStatus).collect(Collectors.toSet());
                            break;
                        case "user":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getName().equals(value1))
                                    .map(LogEntry::getStatus).collect(Collectors.toSet());
                            break;
                        case "date":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getDate().equals(readDate(value1)))
                                    .map(LogEntry::getStatus).collect(Collectors.toSet());
                            break;
                        case "event":
                            set = getEntriesByDate(after, before).stream().filter(e -> e.getEvent().equals(readEvent(value1)))
                                    .map(LogEntry::getStatus).collect(Collectors.toSet());
                            break;
                    }
                    break;
            }
        }
        return set;
    }

    //returns a Date object by its text value
    public Date readDate(String value) {
        Date date = null;
        try {
            date = sdf.parse(value);
        } catch (ParseException ignored) {
        }
        return date;
    }

    //returns an Event object by its text value
    public Event readEvent(String value) {
        Event event = null;
        if (value.equals("LOGIN")) event = Event.LOGIN;
        else if (value.equals("DOWNLOAD_PLUGIN")) event = Event.DOWNLOAD_PLUGIN;
        else if (value.equals("WRITE_MESSAGE")) event = Event.WRITE_MESSAGE;
        else if (value.equals("SOLVE_TASK")) event = Event.SOLVE_TASK;
        else if (value.equals("DONE_TASK")) event = Event.DONE_TASK;
        return event;
    }

    //returns a Status object by its text value
    public Status readStatus(String value) {
        Status status = null;
        if (value.equals("OK")) status = Status.OK;
        else if (value.equals("FAILED")) status = Status.FAILED;
        else if (value.equals("ERROR")) status = Status.ERROR;
        return status;
    }
}
