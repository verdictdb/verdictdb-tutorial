import com.google.common.base.Stopwatch;

import java.sql.*;
import java.util.concurrent.TimeUnit;

/** Created by Dong Young Yoon on 8/3/18. */
public class VerdictTutorialMain {

  public static void main(String[] args) {
    if (args.length < 4) {
      System.out.println("USAGE: VerdictTutorialMain <host> <port> <user> <password> <database> <command>");
      System.out.println("Supported command: create, run");
      return;
    }

    String host, port, database, command, user, password;

    host = "localhost";
    port = "3306";
    database = "tpch1g";
    user = "root";
    password = "";
    command = "";

    if (args.length == 4) {
      // username, password omitted
      host = args[0];
      port = args[1];
      database = args[2];
      command = args[3];
    } else if (args.length == 5) {
      // password omitted
      host = args[0];
      port = args[1];
      user = args[2];
      database = args[3];
      command = args[4];
    } else if (args.length > 5) {
      host = args[0];
      port = args[1];
      user = args[2];
      password = args[3];
      database = args[4];
      command = args[5];
    }

    Connection conn = null;
    Connection mysqlConn = null;

    try {
      Class.forName("com.mysql.jdbc.Driver");
      conn =
          DriverManager.getConnection(
              String.format(
                  "jdbc:verdict:mysql://%s:%s/%s?" + "autoReconnect=true&useSSL=false",
                  host, port, database),
              user, password);
      mysqlConn =
          DriverManager.getConnection(
              String.format("jdbc:mysql://%s:%s/%s", host, port, database), user, password);
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }

    try {
      switch (command.toLowerCase()) {
        case "create":
          createScrambleTable(conn, mysqlConn, database, "lineitem");
          createScrambleTable(conn, mysqlConn, database, "orders");
          break;
        case "run":
          runQuery(conn, mysqlConn, database);
          break;
        case "run2":
          runQuery2(conn, mysqlConn, database);
          break;
        default:
          System.out.println("Unsupported command: " + command);
          System.out.println("Supported command: create, run");
          return;
      }
      conn.close();
      mysqlConn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }

    System.exit(0);
  }

  private static void runQuery(Connection verdictConn, Connection mysqlConn, String database)
      throws SQLException {
    mysqlConn.createStatement().execute("SET GLOBAL query_cache_size=0");
    Stopwatch watch = new Stopwatch();
    watch.start();
    ResultSet rs1 =
        mysqlConn.createStatement()
            .executeQuery(String.format("SELECT avg(l_extendedprice) FROM %s.lineitem", database));
    watch.stop();
    if (rs1.next()) {
      System.out.println("Without VerdictDB: average(l_extendedprice) = " + rs1.getDouble(1));
    }
    long time = watch.elapsedTime(TimeUnit.SECONDS);
    System.out.println("Time Taken = " + time + " s");
    rs1.close();

    watch.reset();
    watch.start();
    ResultSet rs2 =
        verdictConn.createStatement()
            .executeQuery(String.format("SELECT avg(l_extendedprice) FROM %s.lineitem_scramble", database));
    watch.stop();
    if (rs2.next()) {
      System.out.println("With VerdictDB: average(l_extendedprice) = " + rs2.getDouble(1));
    }
    time = watch.elapsedTime(TimeUnit.SECONDS);
    System.out.println("Time Taken = " + time + " s");
    rs2.close();
  }

  private static void runQuery2(Connection verdictConn, Connection mysqlConn, String database)
      throws SQLException {
    mysqlConn.createStatement().execute("SET GLOBAL query_cache_size=0");
    Stopwatch watch = new Stopwatch();
    watch.start();
    ResultSet rs1 =
        mysqlConn.createStatement()
        .executeQuery(String.format("SELECT SUM(l_extendedprice * (1 - l_discount)) "
            + "FROM %s.customer, %s.orders, %s.lineitem "
            + "WHERE c_mktsegment = 'BUILDING'"
            + " AND c_custkey = o_custkey"
            + " AND l_orderkey = o_orderkey", 
            database, database, database));
    watch.stop();
    if (rs1.next()) {
      System.out.println(
          "Without VerdictDB: SUM(l_extendedprice * (1 - l_discount)) = "
              + rs1.getDouble(1));
    }
    long time = watch.elapsedTime(TimeUnit.SECONDS);
    System.out.println("Time Taken = " + time + " s");
    rs1.close();

    watch.reset();
    watch.start();
    ResultSet rs2 =
        verdictConn.createStatement()
        .executeQuery(String.format("SELECT SUM(l_extendedprice * (1 - l_discount)) "
            + "FROM %s.customer, %s.orders_scramble, %s.lineitem_scramble "
            + "WHERE c_mktsegment = 'BUILDING'"
            + " AND c_custkey = o_custkey"
            + " AND l_orderkey = o_orderkey", 
            database, database, database));
    watch.stop();
    if (rs2.next()) {
      System.out.println("With VerdictDB: SUM(l_extendedprice * (1 - l_discount)) = "
          + rs2.getDouble(1));
    }
    time = watch.elapsedTime(TimeUnit.SECONDS);
    System.out.println("Time Taken = " + time + " s");
    rs2.close();
  }

  private static void createScrambleTable(
      Connection verdictConn, Connection mysqlConn, String database, String table) throws SQLException {
    Statement stmt = verdictConn.createStatement();
    String dropQuery = 
        String.format("DROP TABLE IF EXISTS %s.%s_scramble", database, table);
    String createQuery =
        String.format(
            "CREATE SCRAMBLE %s.%s_scramble " + "FROM %s.%s",
            database, table, database, table);
    mysqlConn.createStatement().execute(dropQuery);
    System.out.println(String.format("Creating a scrambled table for %s...", table));
    Stopwatch watch = new Stopwatch();
    watch.start();
    stmt.execute(createQuery);
    stmt.close();
    watch.stop();
    System.out.println(String.format("Scrambled table for %s has been created.", table));
    long time = watch.elapsedTime(TimeUnit.SECONDS);
    System.out.println("Time Taken = " + time + " s");
  }
}
