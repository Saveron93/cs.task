package com.credit;

import java.sql.*;
import java.util.EnumMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Database {

    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final String OUTPUT_TABLE = "CREATE TABLE output (id VARCHAR(20), duration DECIMAL(20), type VARCHAR(20), host VARCHAR(20), alert BIT, PRIMARY KEY (id))";
    private static final String DELETE_OUTPUT = "DROP TABLE IF EXISTS output";
    private static final String HELPER_TABLE = "CREATE TABLE helper (id VARCHAR(20), timestamp DECIMAL(20), PRIMARY KEY (id))";
    private static final String DELETE_HELPER = "DROP TABLE IF EXISTS helper";

    private String name;
    private Connection sql;
    private Map<DB, PreparedStatement> calls = new EnumMap<>(DB.class);

    public enum DB {
        EXISTS_IN_HELPER,
        GET_RESULT_DURATION,
        INSERT_TO_HELPER,
        DELETE_FROM_HELPER,
        INSERT_TO_OUTPUT
    }

    private void prepareStatements() throws SQLException {
        calls.put(DB.EXISTS_IN_HELPER, sql.prepareStatement("SELECT timestamp FROM helper WHERE id=?"));
        calls.put(DB.GET_RESULT_DURATION, sql.prepareStatement("SELECT duration FROM output WHERE id=?"));
        calls.put(DB.INSERT_TO_HELPER, sql.prepareStatement("INSERT INTO helper (id, timestamp) VALUES (?, ?)"));
        calls.put(DB.DELETE_FROM_HELPER, sql.prepareStatement("DELETE FROM helper WHERE id=?"));
        calls.put(DB.INSERT_TO_OUTPUT, sql.prepareStatement("INSERT INTO output (id, duration, type, host, alert) VALUES (?, ?, ?, ?, ?)"));
    }

    public Database(String path) {
        name = "jdbc:hsqldb:file:" + path;
        try {
            Class.forName("org.hsqldb.jdbcDriver");
            sql = DriverManager.getConnection(name, "SA", "");
            Statement stm = sql.createStatement();
            stm.executeUpdate(DELETE_OUTPUT);
            stm.executeUpdate(DELETE_HELPER);
            stm.executeUpdate(OUTPUT_TABLE);
            stm.executeUpdate(HELPER_TABLE);
            prepareStatements();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "An error occurred while creating tables!", e);
        } catch (ClassNotFoundException e) {
            LOGGER.log(Level.SEVERE, "Driver not found!", e);
        }
    }

    private void InsertToHelper(String id, long timestamp) {
        try {
            PreparedStatement stm = calls.get(DB.INSERT_TO_HELPER);
            stm.setString(1, id);
            stm.setLong(2, timestamp);
            stm.executeUpdate();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error on insert to helper!", e);
        }
    }

    private void DeleteFromHelper(String id) {
        try {
            PreparedStatement stm = calls.get(DB.DELETE_FROM_HELPER);
            stm.setString(1, id);
            stm.executeUpdate();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error in delete from helper!", e);
        }
    }

    private long GetTimestampIfExists(String id) {
        try {
            PreparedStatement stm = calls.get(DB.EXISTS_IN_HELPER);
            stm.setString(1, id);
            ResultSet resultSet = stm.executeQuery();
            if (resultSet.next())
                return resultSet.getLong(1);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error on insert to helper!", e);
        }
        return -1;
    }

    private void InsertToOutput(String id, long duration, Event.Type type, String host, boolean alert) {
        try {
            PreparedStatement stm = calls.get(DB.INSERT_TO_OUTPUT);
            stm.setString(1, id);
            stm.setLong(2, duration);
            stm.setString(3, type.toString());
            stm.setString(4, host);
            stm.setBoolean(5, alert);
            stm.executeUpdate();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error on insert to output!", e);
        }
    }

    public long GetResultDuration(String id) {
        try {
            PreparedStatement stm = calls.get(DB.GET_RESULT_DURATION);
            stm.setString(1, id);
            ResultSet resultSet = stm.executeQuery();
            if (resultSet.next())
                return resultSet.getLong(1);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error on insert to output!", e);
        }
        return -1;
    }

    public void ProcessEvent(Event event) {
        long timestamp = GetTimestampIfExists(event.id);
        if (timestamp >= 0) {
            DeleteFromHelper(event.id);
            long duration = (event.state == Event.State.FINISHED) ? event.timestamp - timestamp : timestamp - event.timestamp;
            InsertToOutput(event.id, duration, event.type, event.host, duration > 4);
        } else
            InsertToHelper(event.id, event.timestamp);
    }

    public void Shutdown() {
        try {
            Statement st = sql.createStatement();
            st.execute(DELETE_HELPER);
            st.execute("SHUTDOWN");
            st.close();
            sql.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
