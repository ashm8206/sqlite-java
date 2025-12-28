
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.Arrays;

import StorageEngine.StorageEngine;

public class Main {
  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Missing <database path> and <command>");
      printUsage();
      return;
    }

    String databaseFilePath = args[0];
    String command = args[1];

    try {
      // Create storage engine instance
      StorageEngine storageEngine = new StorageEngine(databaseFilePath);

      // Process command
      switch (command) {
        case ".dbinfo" -> handleDbInfo(storageEngine);
        case ".tables" -> handleTables(storageEngine);
        case ".exit" -> handleExit();
        default -> executeSQLCommand(storageEngine, command);
      }
      ;

      // Clean up
      storageEngine.close();

    } catch (IOException e) {
      System.err.println("Error reading database: " + e.getMessage());
      System.exit(1);
    }

  }

  /**
   * Handles the .dbinfo command - displays database information.
   */
  private static void handleDbInfo(StorageEngine storageEngine) throws IOException {
    Map<String, Object> dbInfo = storageEngine.getDatabaseInfo();

    System.out.println("database page size: " + dbInfo.get("pageSize"));
    System.out.println("number of tables: " + dbInfo.get("tableCount"));

    // Debug logging
    System.err.println("Database info retrieved successfully");
  }

  /**
   * Handles the .tables command - lists all tables in the database.
   */
  private static void handleTables(StorageEngine storageEngine) throws IOException {
    List<String> tableNames = storageEngine.getTableNames();

    if (tableNames.isEmpty()) {
      System.out.println("No tables found in database.");
    } else {
      // SQLite's .tables command shows tables space-separated
      System.out.println(String.join(" ", tableNames));
    }

    // Debug logging
    System.err.println("Found " + tableNames.size() + " table(s)");
  }

  /**
   * Handles the .exit command.
   */
  private static void handleExit() {
    System.out.println("Exiting the program.");
    System.exit(0);
  }

  /**
   * Handles unknown commands.
   */
  private static void executeSQLCommand(StorageEngine engine, String sql) throws IOException {
    String trimmed = sql.trim();
    String lower = trimmed.toLowerCase();

    if (lower.startsWith("select count(*) from")) {
      String tableName = extractTableName(trimmed, "SELECT COUNT(*) FROM");
      int count = engine.countRowsInTable(engine.getTableRootPage(tableName));
      System.out.println(count);

    } else if (lower.startsWith("select ") && lower.contains(" from ")) {
      // Case-insensitive positions
      int fromPos = lower.indexOf(" from ");
      int wherePos = lower.indexOf(" where ", fromPos + 6);

      // Extract parts using positions from lower, but substring from original trimmed
      String columnsPart = trimmed.substring(7, fromPos).trim();

      String tableName;
      String whereClause = null;

      if (wherePos != -1) {
        tableName = trimmed.substring(fromPos + 6, wherePos).trim();
        whereClause = trimmed.substring(wherePos + 7).trim();
      } else {
        tableName = trimmed.substring(fromPos + 6).trim();
      }

      tableName = tableName.replaceAll(";+$", "").trim();
      if (tableName.isEmpty()) {
        throw new IOException("Missing table name");
      }

      List<String> columnNames = extractColumnNames(columnsPart);

      String filterColumn = null;
      String filterValue = null;

      if (whereClause != null) {
        whereClause = whereClause.replaceAll(";+$", "").trim();

        var matcher = Pattern.compile("^(\\w+)\\s*=\\s*([\"'])([^\"']*)\\2$")
            .matcher(whereClause);
        if (!matcher.matches()) {
          throw new IOException("Only WHERE column = 'value' or \"value\" supported: " + whereClause);
        }
        filterColumn = matcher.group(1);
        filterValue = matcher.group(3);
      }

      List<String> rows = engine.getRowsWithFilters(tableName, columnNames, filterColumn, filterValue);
      for (String row : rows) {
        System.out.println(row);
      }
    } else {
      throw new IOException("Unsupported command: " + sql);
    }
  }

  private static List<String> extractColumnNames(String columnString) throws IOException {
    columnString = columnString.trim();
    if (columnString.isEmpty()) {
      throw new IOException("Missing column names");
    }
    List<String> columns = Arrays.asList(columnString.split(","));
    return columns.stream().map(String::trim).toList();
  }

  private static String extractTableName(String sql, String prefix) throws IOException {
    String tablePart = sql.substring(prefix.length()).trim().replaceAll(";+$", "").trim();
    if (tablePart.isEmpty()) {
      throw new IOException("Missing table name");
    }
    return tablePart;
  }

  /**
   * Prints usage information.
   */

  private static void printUsage() {
    System.out.println("Usage: java Main <database path> <command>");
    System.out.println("Commands:");
    System.out.println("  .dbinfo   - Display database information");
    System.out.println("  .tables   - List all tables in the database");
    System.out.println("  .exit     - Exit the program");
  }
}
