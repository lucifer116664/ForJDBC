import util.ConnectionManager;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import java.sql.*;

public class Main {
    public static void main(String[] args) throws SQLException {
        System.out.println("---------------------------------------");
        statementThings();
        preparedStatementThings();
        callableStatementThings();
        transactions();
        cachedRowSetThings();
        metadata();
    }

    private static void statementThings() {
        String sqlQueryDDL = """
        DROP TABLE IF EXISTS users;
        CREATE TABLE users (
        user_id serial PRIMARY KEY,
        login varchar(20) NOT NULL,
        password varchar(20) NOT NULL
        );
        """;

        try (var connection = ConnectionManager.getConnection();
             //statement - query without params ONLY
             var statement = connection.createStatement()) {

            //execute() - DDL;
            statement.execute(sqlQueryDDL);
///////////////////////////////////////////////////////////////////////////////////////////////////////
            String sqlQueryInsert1 = "INSERT INTO users(login, password) VALUES ('a', 'a')";
            String sqlQueryInsert2 = "INSERT INTO users(login, password) VALUES ('b', 'b')";

            //some queries at the same time
            statement.addBatch(sqlQueryInsert1);
            statement.addBatch(sqlQueryInsert2);
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void preparedStatementThings() {
        String sqlQueryParams = "INSERT INTO users(login, password) VALUES (?, ?)";

        try(var connection = ConnectionManager.getConnection();
            //has protection from SQL injection
            var preparedStatement = connection.prepareStatement(sqlQueryParams)) {

            //set params
            preparedStatement.setString(1, "Ya");
            preparedStatement.setString(2, "Yaya");
            //executeUpdate - insert, update, delete
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void callableStatementThings() {
        String sqlQueryCallable = "CALL insert_data_in_users(?, ?)";

        try(var connection = ConnectionManager.getConnection();
            //for procedures saved in DB
            var callableStatement = connection.prepareCall(sqlQueryCallable)) {

            callableStatement.setString(1, "mama");
            callableStatement.setString(2, "mama123");

            callableStatement.executeUpdate();
        }catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static ResultSet resultSetThings() {
        try(var connection = ConnectionManager.getConnection();
            //statement - query without params ONLY
            var statement = connection.createStatement()) {

            //result set saves the result, executeQuery to save it
            ResultSet resultSet = statement.executeQuery("SELECT * FROM users");

            /*System.out.println("resultSetThings():");
            //every iteration - next row
            while (resultSet.next()) {//resultSet.previous() .relative(-3) needs statement parameter(ResultSet.TYPE_)
                //every column you need
                int id = resultSet.getInt("user_id");
                String login = resultSet.getString("login");
                String password = resultSet.getString("password");
                System.out.printf("ID: %d, Login: %s, Password: %s\n", id, login, password);
            }
            System.out.println("---------------------------------------");*/

            //forCachedRowSet
            RowSetFactory factory = RowSetProvider.newFactory();
            CachedRowSet cachedRowSet = factory.createCachedRowSet();
            cachedRowSet.populate(resultSet);

            return cachedRowSet;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void cachedRowSetThings() throws SQLException {
        ResultSet resultSet = resultSetThings();

        System.out.println("cachedSetThings():");

        while (resultSet.next()) {
            int id = resultSet.getInt("user_id");
            String login = resultSet.getString("login");
            String password = resultSet.getString("password");
            System.out.printf("ID: %d, Login: %s, Password: %s\n", id, login, password);
        }
        System.out.println("---------------------------------------");

        System.out.println("cachedRowSet command:");

        CachedRowSet cachedRowSet2 = (CachedRowSet) resultSet;
        cachedRowSet2.setUrl(ConnectionManager.getURL());
        cachedRowSet2.setUsername(ConnectionManager.getUsername());
        cachedRowSet2.setPassword(ConnectionManager.getPassword());
        cachedRowSet2.setCommand("SELECT * FROM users WHERE user_id = ?");
        cachedRowSet2.setInt(1, 4);
        cachedRowSet2.setPageSize(20);
        cachedRowSet2.execute();
        do {
            while (cachedRowSet2.next()) {
                //every column you need
                int id = cachedRowSet2.getInt("user_id");
                String login = cachedRowSet2.getString("login");
                String password = cachedRowSet2.getString("password");
                System.out.printf("ID: %d, Login: %s, Password: %s\n", id, login, password);
            }
        } while(cachedRowSet2.nextPage());
        System.out.println("---------------------------------------");
    }

    private static void metadata() {
        System.out.println("metadata():");
        try(var connection = ConnectionManager.getConnection();
            var statement = connection.createStatement()) {

            DatabaseMetaData databaseMetaData = connection.getMetaData();
            // Get tables metadata
            String[] types = {"TABLE"};
            ResultSet resultSet = databaseMetaData.getTables(null, null, null, types);

            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                String tableSchema = resultSet.getString("TABLE_SCHEM");
                String tableCatalog = resultSet.getString("TABLE_CAT");
                String tableType = resultSet.getString("TABLE_TYPE");

                System.out.println("Table Name: " + tableName);
                System.out.println("Table Schema: " + tableSchema);
                System.out.println("Table Catalog: " + tableCatalog);
                System.out.println("Table Type: " + tableType);
                System.out.println("---------------------------------------");
            }
///////////////////////////////////////////////////////////////////////////////////////////////////////
            resultSet = statement.executeQuery("SELECT * FROM users");
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            int columnCount = resultSetMetaData.getColumnCount();

            String columnName = resultSetMetaData.getColumnName(1);

            int columnType = resultSetMetaData.getColumnType(1);

            String columnTypeName = resultSetMetaData.getColumnTypeName(1);

            int columnSize = resultSetMetaData.getColumnDisplaySize(2);

            int precision = resultSetMetaData.getPrecision(1);

            //Scale(numbers after dot)
            //int scale = resultSetMetaData.getScale(1);

            int isNullable = resultSetMetaData.isNullable(2);

            System.out.printf("Number of columns: %d\nColumn name: %s\nData type(const number): %d\n" +
                            "Data type name: %s\nColumn size(varchar): %d\nPrecision: %d\nIs nullable?: %d\n",
                    columnCount, columnName, columnType, columnTypeName, columnSize, precision, isNullable);
            System.out.println("---------------------------------------");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void transactions() {
        String sqlQueryInsert1 = "INSERT INTO users(login, password) VALUES ('c', 'c')";
        String sqlQueryInsert2 = "INSERT INTO users(login, password) VALUES ('d', 'd')";
        String sqlQueryInsert3 = "INSERT INTO users(login, password) VALUES ('e', 'e')";
        String sqlQueryInsert4 = "INSERT INTO users(login, password) VALUES ('f', 'f')";

        Connection connection = null;
        //Savepoint savepoint = null;
        try {
            connection = ConnectionManager.getConnection();
            var statement = connection.createStatement();

            connection.setAutoCommit(false);

            //transaction - sql queries executed like monolith block
            statement.executeUpdate(sqlQueryInsert1);
            statement.executeUpdate(sqlQueryInsert2);
            //savepoint = connection.setSavepoint();
            statement.executeUpdate(sqlQueryInsert3);
            statement.executeUpdate(sqlQueryInsert4);

            //commit changes
            connection.commit();

            statement.close();
            connection.close();
        } catch (SQLException e) {
            try {
                //if something goes wrong - rollback to las commit
                connection.rollback();

                //rollback to savepoint and commit
                //connection.rollback(savepoint);
                //connection.commit();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}