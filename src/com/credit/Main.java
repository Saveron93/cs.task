package com.credit;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class Main {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final int LOG_INTERVALS = 10;

    private static void ParseFile(String filename, Database database) throws IOException {
        Gson gson = new GsonBuilder().create();
        FileInputStream inputStream = new FileInputStream(filename);
        Scanner sc = new Scanner(inputStream, "UTF-8");

        int line = 0;
        while (sc.hasNextLine()) {
            line++;
            Event event = gson.fromJson(sc.nextLine(), Event.class);
            database.ProcessEvent(event);
            if (line % LOG_INTERVALS == 0)
                LOGGER.info("Processed lines: " +  line);
        }
    }

    public static void main(String[] args) {
        if (args.length == 0)
            throw new IllegalArgumentException("Please provide path to input file");
        if (args.length == 1)
            LOGGER.warning("Output file not provided, default 'output' will be used");
        else if (args.length > 2)
            LOGGER.warning("Too many arguments provided, just first two will be used");

        String output = (args.length > 1) ? args[1] : "output";
        Database database = new Database("out");
        try {
            LOGGER.info("Processing file: " + output);
            ParseFile(args[0], database);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "File not found or has wrong format: " +  args[0], e);
            throw new IllegalArgumentException("File not found or has wrong format");
        }
        database.Shutdown();
    }
}
