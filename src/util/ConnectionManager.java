package util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public final class ConnectionManager {
    private static final String URL_KEY = "db.url";
    private static final String USERNAME_KEY = "db.username";
    private static final String PASSWORD_KEY = "db.password";

    public static Connection getConnection(){
        try {
            return DriverManager.getConnection(
                    PropertiesUtil.getProperty(URL_KEY),
                    PropertiesUtil.getProperty(USERNAME_KEY),
                    PropertiesUtil.getProperty(PASSWORD_KEY)
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getURL() {
        return PropertiesUtil.getProperty(URL_KEY);
    }

    public static String getUsername() {
        return PropertiesUtil.getProperty(USERNAME_KEY);
    }

    public static String getPassword() {
        return PropertiesUtil.getProperty(PASSWORD_KEY);
    }
}
