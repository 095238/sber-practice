package com.jdbclient;

import java.sql.*;
import java.util.Properties;
import java.util.Scanner;
import java.io.InputStream;

public class App {
    public static void main(String[] args) {
        // Загрузка конфигурации
        Properties config = loadConfig();
        if (config == null) {
            System.err.println("Ошибка: Не удалось загрузить конфигурацию");
            return;
        }

        // Подключение к БД
        try (Connection conn = DriverManager.getConnection(
                config.getProperty("db.url"),
                config.getProperty("db.user"),
                config.getProperty("db.password"))) {

            System.out.println("Подключение установлено, введите SQL выражение");
            Scanner scanner = new Scanner(System.in);

            while (true) {
                System.out.print("> ");
                String sql = scanner.nextLine().trim();

                if (sql.equalsIgnoreCase("QUIT")) {
                    break;
                }

                if (sql.isEmpty()) {
                    continue;
                }

                executeSQL(conn, sql);
            }

        } catch (SQLException e) {
            System.err.println("Ошибка подключения к БД: " + e.getMessage());
        }
    }

    private static Properties loadConfig() {
        Properties props = new Properties();
        try (InputStream input = App.class.getClassLoader()
                .getResourceAsStream("config.properties")) {

            if (input == null) {
                System.err.println("Файл config.properties не найден");
                return null;
            }

            props.load(input);
            return props;

        } catch (Exception e) {
            System.err.println("Ошибка чтения конфигурации: " + e.getMessage());
            return null;
        }
    }

    private static void executeSQL(Connection conn, String sql) {
        try (Statement stmt = conn.createStatement()) {

            // Для SELECT используем ограничение выборки
            if (isSelectQuery(sql)) {
                stmt.setMaxRows(11); // Получаем на 1 запись больше для проверки

                try (ResultSet rs = stmt.executeQuery(sql)) {
                    printResultSet(rs);
                }
            }
            // Для остальных команд (INSERT/UPDATE/DELETE)
            else {
                int count = stmt.executeUpdate(sql);
                System.out.println("Выполнено. Затронуто строк: " + count);
            }

        } catch (SQLException e) {
            System.err.println("Ошибка выполнения SQL: " + e.getMessage());
        }
    }

    private static boolean isSelectQuery(String sql) {
        return sql.trim().toUpperCase().startsWith("SELECT");
    }

    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int colCount = meta.getColumnCount();

        // Вывод заголовков
        for (int i = 1; i <= colCount; i++) {
            System.out.printf("%-20s", meta.getColumnName(i));
        }
        System.out.println();

        // Вывод данных
        int rowCount = 0;
        while (rs.next() && rowCount < 10) {
            for (int i = 1; i <= colCount; i++) {
                System.out.printf("%-20s", rs.getString(i));
            }
            System.out.println();
            rowCount++;
        }

        // Проверка наличия дополнительных записей
        if (rs.next()) {
            System.out.println("В БД есть еще записи");
        }
    }
}