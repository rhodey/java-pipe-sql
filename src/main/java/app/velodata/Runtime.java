package app.velodata;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class Runtime {

  private ExecutorService exec;
  private LinkedBlockingDeque<String> output;
  private HikariDataSource pool;
  private Queries queries;

  private final Map<Integer, Connection> connections = new ConcurrentHashMap<>();
  private final Set<Integer> txns = new ConcurrentSkipListSet<>();

  private String readQueryId(Integer connection, List<String> args) throws AppException {
    if (args.isEmpty()) { throw new AppException(connection, "read query id - wrong number of args"); }
    if (args.get(0).isEmpty()) { throw new AppException(connection, "read query id - query id is empty"); }
    return args.get(0);
  }

  private String readQuery(Integer connection, String requestId, List<String> args) throws AppException {
    if (args.size() < 2) { throw new AppException(connection, requestId, "read query - wrong number of args"); }
    if (!args.get(1).startsWith("s")) { throw new AppException(connection, requestId, "read query - missing 's' before base64"); }
    String query;
    try {
      query = args.get(1);
      byte[] decoded = Base64.getDecoder().decode(query.substring(1));
      query = new String(decoded);
    } catch (Exception e) {
      throw new AppException(connection, requestId, "read query - base64 decode failed");
    }
    if (query.isEmpty()) { throw new AppException(connection, requestId, "read query - query is empty"); }
    return query;
  }

  private String readCommand(Integer connection, List<String> args) throws AppException {
    if (args.isEmpty()) { throw new AppException(connection, "read cmd - wrong number of args"); }
    String command = args.get(0);
    switch (command) {
      case "connect":
      case "query":
      case "begin":
      case "commit":
      case "rollback":
      case "close":
        return command;
      default:
        throw new AppException(connection, "read cmd - invalid command: " + command);
    }
  }

  private Integer readConnectionNumber(String input) throws AppException {
    if (input.split(",")[0].isEmpty()) { throw new AppException("input has no connection number"); }
    try {
      return Integer.parseInt(input.split(",")[0]);
    } catch (NumberFormatException e) {
      throw new AppException("input has invalid connection number");
    }
  }

  private void onInput(String input) throws AppException {
    if (input.equals("boot")) {
      queueOutput("boot");
      return;
    }

    Integer connNum = readConnectionNumber(input);
    String[] parts = input.split(",", -1);
    List<String> args = List.of(parts);
    args = args.subList(1, args.size());

    String command = readCommand(connNum, args);
    args = args.subList(1, args.size());

    try {

      Connection connection = connections.get(connNum);

      switch (command) {
        case "connect":
          if (connection != null) { throw new AppException(connNum, "preventing double connect"); }
          exec.submit(() -> {
            try {
              Connection newConnection = pool.getConnection();
              connections.put(connNum, newConnection);
              queueOutput(connNum + "," + command);
            } catch (Exception e) {
              if (e.getMessage().contains("timed out")) {
                queueError(connNum, null, "timeout exceeded when trying to connect");
              } else {
                queueError(connNum, null, e);
              }
            }
          });
          break;

        case "query":
          if (connection == null) { throw new AppException(connNum, "query before connect"); }
          String queryId = readQueryId(connNum, args);
          String query = readQuery(connNum, queryId, args);
          args = args.subList(2, args.size());
          queries.queue(connNum, queryId, connection, query, args);
          break;

        case "begin":
          if (connection == null) { throw new AppException(connNum, "begin before connect"); }
          if (txns.contains(connNum)) { throw new AppException(connNum, "preventing double begin"); }
          exec.submit(() -> {
            try {
              connection.setAutoCommit(false);
              txns.add(connNum);
              queueOutput(connNum + "," + command);
            } catch (Exception e) {
              queueError(connNum, null, e);
            }
          });
          break;

        case "commit":
          if (!txns.contains(connNum)) { throw new AppException(connNum, "commit before begin"); }
          if (connection == null) { throw new AppException(connNum, "connection null at commit"); }
          exec.submit(() -> {
            try {
              connection.commit();
              connection.setAutoCommit(true);
              txns.remove(connNum);
              queueOutput(connNum + "," + command);
            } catch (Exception e) {
              queueError(connNum, null, e);
            }
          });
          break;

        case "rollback":
          if (connection == null || !txns.contains(connNum)) { return; }
          exec.submit(() -> {
            try {
              txns.remove(connNum);
              connection.rollback();
              connection.setAutoCommit(true);
              queueOutput(connNum + "," + command);
            } catch (Exception e) {
              queueError(connNum, null, e);
            }
          });
          break;

        case "close":
          if (connection == null) { return; }
          txns.remove(connNum);
          connections.remove(connNum);
          connection.close();
          break;
      }

    } catch (Exception e) {
      throw new AppException(connNum, e);
    }
  }

  private void queueOutput(String data) {
    output.add("o:" + data);
  }

  private void queueStackTrace(Exception e) {
    StringWriter stack = new StringWriter();
    e.printStackTrace(new PrintWriter(stack));
    String info = "i," + stack;
    info = info.replace("\n", " ");
    output.add("i:" + info);
  }

  private void handleClose(Integer connNum) {
    try {
      txns.remove(connNum);
      Connection conn = connections.remove(connNum);
      if (conn != null) { conn.close(); }
    } catch (Exception ignore) { }
  }

  private void queueError(Integer connNum, String queryId, String error) {
    error = error.replace("\n", " ").replace(",", " ");
    if (error.toLowerCase().contains("closed")) {
      if (connNum >= 0) {
        handleClose(connNum);
        output.add("e:" + connNum + ",closed");
      } else {
        output.add("e:*,closed");
      }
      output.add("i:i,connection " + connNum + " closed unexpectedly");
      return;
    }
    String out = connNum + "";
    if (connNum < 0) { out = "*"; }
    if (queryId != null) { out += "," + queryId; }
    output.add("e:" + out + "," + error);
  }

  private void queueError(Integer connNum, String queryId, Exception e) {
    String error = e.getMessage();
    if (error == null) { error = e.getClass().getName(); }
    queueError(connNum, queryId, error);
    queueStackTrace(e);
  }

  private void onError(AppException e) {
    Integer connNum = e.getConnection();
    if (connNum == null) {
      queueError(-1, e.getQuery(), e);
    } else {
      queueError(connNum, e.getQuery(), e);
    }
  }

  private class KeepAliveTask implements Runnable {
    private final int queryTimeout;
    public KeepAliveTask(int queryTimeout) { this.queryTimeout = queryTimeout; }

    @Override
    public void run() {
      for (Integer conNum : connections.keySet()) {
        Connection conn = connections.get(conNum);
        try {
          if (conn.isClosed()) {
            txns.remove(conNum);
            connections.remove(conNum);
            continue;
          }
          if (conn.isValid(queryTimeout)) { continue; }
          queueError(conNum, null, "closed");
        } catch (Exception e) {
          queueError(conNum, null, "closed");
        }
      }
    }
  }

  private void run() {
    int queryTimeout;
    long keepAliveMs;

    try {

      String threads = System.getenv("threads");
      exec = Executors.newFixedThreadPool(Integer.parseInt(threads));
      output = new LinkedBlockingDeque<>();
      exec.submit(new Output(output));

    } catch (NumberFormatException e) {
      System.err.println("*,failed to parse env var to number");
      return;
    }

    try {

      String url = System.getenv("jdbc_url");
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(url);
      config.setUsername(System.getenv("user"));
      config.setPassword(System.getenv("password"));

      String maxConnections = System.getenv("max");
      config.setMaximumPoolSize(Integer.parseInt(maxConnections));

      String connTimeout = System.getenv("connection_timeout_millis");
      config.setConnectionTimeout(Long.parseLong(connTimeout));

      String idleTimeout = System.getenv("idle_timeout_millis");
      config.setIdleTimeout(Long.parseLong(idleTimeout));

      String queryTimeoutS = System.getenv("query_timeout");
      queryTimeout = Integer.parseInt(queryTimeoutS);
      queryTimeout = (int) Math.ceil(queryTimeout / 1000.0);

      String keepAlive = System.getenv("keep_alive_millis");
      keepAliveMs = Long.parseLong(keepAlive);
      pool = new HikariDataSource(config);
      queries = new Queries(exec, output, connections, txns, queryTimeout);

    } catch (NumberFormatException e) {
      onError(new AppException("failed to parse env var to number"));
      return;
    } catch (Exception e) {
      onError(new AppException(e));
      return;
    }

    ScheduledExecutorService repeater = Executors.newScheduledThreadPool(1);
    if (keepAliveMs > 0) { repeater.scheduleAtFixedRate(new KeepAliveTask(queryTimeout), keepAliveMs, keepAliveMs, TimeUnit.MILLISECONDS); }

    Scanner stdin = new Scanner(System.in);
    while (stdin.hasNext()) {
      try {
        onInput(stdin.nextLine());
      } catch (AppException e) {
        onError(e);
      }
    }

    exec.shutdownNow();
    repeater.shutdownNow();
    pool.close();
  }

  public static void main(String[] args) {
    new Runtime().run();
  }

}
