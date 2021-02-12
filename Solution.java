package com.javarush.task.task39.task3913;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

public class Solution {
    public static void main(String[] args) throws ParseException, IOException {
        LogParser logParser = new LogParser(Paths.get("C:\\Users\\Alexey\\Downloads\\JavaRushTasks\\4.JavaCollections\\src\\com\\javarush\\task\\task39\\task3913\\logs"));
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            String query = in.readLine();
            if (query.equals("e")) break;
            Set<Object> set = logParser.execute(query);
            System.out.println(set);
        }
    }
}