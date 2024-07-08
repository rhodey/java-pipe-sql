package app.velodata;

import org.postgresql.util.PGobject;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.*;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

public class Queries {

    private final ExecutorService threads;
    private final LinkedBlockingDeque<String> output;
    private final Map<Integer, Connection> connections;
    private final Set<Integer> txns;
    private final int queryTimeout;

    public Queries(ExecutorService threads, LinkedBlockingDeque<String> output, Map<Integer, Connection> connections, Set<Integer> txns, int queryTimeout) {
        this.threads = threads;
        this.output = output;
        this.connections = connections;
        this.txns = txns;
        this.queryTimeout = queryTimeout;
    }

    private boolean isUpdate(String query) {
        String test = query.trim().toUpperCase();
        return test.startsWith("CREATE") || test.startsWith("ALTER") ||  test.startsWith("DROP") ||
                test.startsWith("INSERT") || test.startsWith("UPDATE") || test.startsWith("DELETE") || test.startsWith("LOCK");
    }

    private boolean isReturning(String query) {
        return query.toUpperCase().contains("RETURNING");
    }

    private boolean isSupportedType(String type) {
        return type.equals("text") || type.equals("jsonb") || type.equals("varchar") || type.equals("timestamptz") ||
                type.equals("int4") || type.equals("int8") || type.equals("numeric") || type.equals("bigserial") ||
                type.equals("float8") || type.equals("bool") || type.equals("void");
    }

    private String encodeToString(Object output) {
        if (output == null) { return ""; }
        return output.toString();
    }

    private List<Integer> findIndexes(String str, String find) {
        List<Integer> results = new LinkedList<>();
        int idx = str.indexOf(find);
        if (idx < 0) { return results; }
        while (idx >= 0) {
            results.add(idx);
            idx = str.indexOf(find, idx + 1);
        }
        return results;
    }

    private void bindArg(PreparedStatement stmt, String type, int idx, String arg) throws SQLException {
        if (!isSupportedType(type)) { throw new SQLException("bind arg - unsupported sql type " + type); }
        Object obj = null;
        if (arg.isEmpty() || type.equals("void")) {
            stmt.setObject(idx, null);
            return;
        }
        try {
            switch (type) {
                case "text":
                case "jsonb":
                case "varchar":
                case "timestamptz":
                    if (arg.startsWith("s")) {
                        byte[] decoded = Base64.getDecoder().decode(arg.substring(1));
                        if (new String(decoded).equals("\"\"")) { decoded = new byte[0]; }
                        obj = new String(decoded);
                        stmt.setObject(idx, obj);
                    } else if (arg.startsWith("j")) {
                        byte[] decoded = Base64.getDecoder().decode(arg.substring(1));
                        PGobject pgObject = new PGobject();
                        pgObject.setType("json");
                        pgObject.setValue(new String(decoded));
                        stmt.setObject(idx, pgObject);
                    } else if (arg.startsWith("t")) {
                        stmt.setTimestamp(idx, Timestamp.from(Instant.parse(arg.substring(1))));
                    } else {
                        stmt.setObject(idx, arg);
                    }
                    break;
                case "bool":
                    obj = arg.equals("true") ? Boolean.TRUE : Boolean.FALSE;
                    stmt.setObject(idx, obj);
                    break;
                case "int4":
                    obj = Integer.parseInt(arg);
                    stmt.setObject(idx, obj);
                    break;
                case "int8":
                case "numeric":
                case "bigserial":
                    obj = Long.parseLong(arg);
                    stmt.setObject(idx, obj);
                    break;
                case "float8":
                    obj = Double.parseDouble(arg);
                    stmt.setObject(idx, obj);
                    break;
            }
        } catch (NumberFormatException e) {
            throw new SQLException("bind arg - failed to parse idx " + idx + " to number");
        } catch (DateTimeParseException e) {
            throw new SQLException("bind arg - failed to parse idx " + idx + " to timestamp");
        } catch (Exception e) {
            throw new SQLException("bind arg - failed to base64 decode idx " + idx);
        }
    }

    private String[] readRow(String[] cols, ResultSet from) throws SQLException {
        String[] row = new String[cols.length];
        for (int i = 0; i < cols.length; i++) {
            String col = cols[i].split(":")[0];
            String type = cols[i].split(":")[1];
            if (!isSupportedType(type)) { throw new SQLException("read row - unsupported col type " + col + " = " + type); }
            switch (type) {
                case "text":
                case "jsonb":
                case "varchar":
                    row[i] = from.getString(i+1);
                    if (row[i] != null && row[i].isEmpty()) { row[i] = "\"\""; }
                    if (row[i] != null) { row[i] = "s" + Base64.getEncoder().encodeToString(row[i].getBytes()); }
                    row[i] = encodeToString(row[i]);
                    break;
                case "timestamptz":
                    row[i] = encodeToString(from.getString(i+1));
                    break;
                case "bool":
                    row[i] = encodeToString(from.getObject(i+1, Boolean.class));
                    break;
                case "int4":
                    row[i] = encodeToString(from.getObject(i+1, Integer.class));
                    break;
                case "int8":
                case "numeric":
                case "bigserial":
                    row[i] = encodeToString(from.getObject(i+1, Long.class));
                    break;
                case "float8":
                    row[i] = encodeToString(from.getObject(i+1, Double.class));
                    break;
                case "void":
                    row[i] = encodeToString(null);
                    break;
            }
        }
        return row;
    }

    private static class BindArg implements Comparable<BindArg> {
        final String arg;
        final int index;
        public BindArg(String arg, int index) {
            this.arg = arg;
            this.index = index;
        }
        @Override
        public int compareTo(BindArg other) {
            return this.index - other.index;
        }
    }

    // translate SELECT $1, $2, $3 to SELECT ?, ?, ?
    private List<String> prepQuery(String query, List<String> args) throws SQLException {
        String queryOut = query;
        List<BindArg> lookup = new LinkedList<>();
        for (int i = args.size(); i > 0; i--) {
            String token = "\\$"+i;
            String test = token.replace("\\", "");
            if (!queryOut.contains(test)) { throw new SQLException("query args do not match template string: " + query); }
            List<Integer> indexes = findIndexes(queryOut, test);
            for (Integer idx : indexes) { lookup.add(new BindArg(args.get(i-1), idx)); }
            queryOut = queryOut.replaceAll(token, "?");
        }
        if (queryOut.contains("$")) { throw new SQLException("query args do not match template string: " + query); }
        List<String> result = new LinkedList<>();
        Collections.sort(lookup);
        result.add(queryOut);
        for (BindArg arg : lookup) { result.add(arg.arg); }
        return result;
    }

    public void queue(Integer connNum, String queryId, Connection conn, String query, List<String> args) throws AppException {
        try {

            args = prepQuery(query, args);
            query = args.get(0);
            args = args.subList(1, args.size());
            threads.submit(new QueryTask(connNum, queryId, conn, query, args));

        } catch (Exception e) {
            throw new AppException(connNum, queryId, e);
        }
    }

    private class QueryTask implements Runnable {
        private final Integer connNum;
        private final String queryId;
        private final Connection conn;
        private final String query;
        private final List<String> args;

        public QueryTask(Integer connNum, String queryId, Connection conn, String query, List<String> args) {
            this.connNum = connNum;
            this.queryId = queryId;
            this.conn = conn;
            this.query = query;
            this.args = args;
        }

        private List<String[]> run(PreparedStatement stmt) throws SQLException {
            List<String[]> result = new LinkedList<>();

            // bind args
            ParameterMetaData params = stmt.getParameterMetaData();
            for (int i = 0; i < params.getParameterCount(); i++) {
                String type = params.getParameterTypeName(i + 1);
                bindArg(stmt, type, i + 1, args.get(i));
            }

            // exec update without RETURNING keyword
            if (isUpdate(query) && !isReturning(query)) {
                int count = stmt.executeUpdate();
                // first item is cols + types (none in this case)
                result.add(null);
                // second item is update count and row count
                String[] counts = new String[]{""+count, "0"};
                result.add(counts);
                return result;
            }

            // exec update with RETURNING or select
            ResultSet rows = stmt.executeQuery();

            // add column names and types to result
            ResultSetMetaData meta = rows.getMetaData();
            String[] cols = new String[meta.getColumnCount()];
            int c = 0;
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String columnName = meta.getColumnName(i);
                String columnType = meta.getColumnTypeName(i);
                if (isSupportedType(columnName)) { columnName = ++c + ""; }
                else if (columnName.startsWith("?")) { columnName = ++c + ""; }
                cols[i-1] = columnName + ":" + columnType;
            }
            result.add(cols);

            // second item is update count and row count
            String[] counts = new String[]{"0", "0"};
            result.add(counts);

            // add rows to result
            int count = 0;
            while (rows.next()) {
                result.add(readRow(cols, rows));
                count++;
            }

            // second item is update count and row count
            if (isReturning(query)) { result.get(1)[0] = ""+count; }
            result.get(1)[1] = ""+count;
            return result;
        }

        private void queueOutput(String data) {
            output.add("o:" + connNum + "," + queryId + data);
        }

        private void queueStackTrace(Exception e) {
            StringWriter stack = new StringWriter();
            e.printStackTrace(new PrintWriter(stack));
            String info = "i," + stack;
            info = info.replace("\n", " ");
            output.add("i:" + info);
        }

        private void handleClose() {
            try {
                txns.remove(connNum);
                Connection conn = connections.remove(connNum);
                if (conn != null) { conn.close(); }
            } catch (Exception ignore) { }
        }

        private void queueError(String error) {
            error = error.replace("\n", " ").replace(",", " ");
            if (error.toLowerCase().contains("closed")) {
                handleClose();
                output.add("e:" + connNum + ",closed");
                output.add("i:i,connection " + connNum + " closed unexpectedly");
                return;
            }
            output.add("e:" + connNum + "," + queryId + "," + error);
        }

        private void queueError(Exception e) {
            String error = e.getMessage();
            if (error == null) { error = e.getClass().getName(); }
            queueError(error);
            queueStackTrace(e);
        }

        @Override
        public void run() {
            try (PreparedStatement stmt = conn.prepareStatement(query)) {
                stmt.setQueryTimeout(queryTimeout);

                List<String[]> result = run(stmt);
                String[] cols = result.get(0);
                String[] counts = result.get(1);
                List<String[]> rows = result.subList(2, result.size());

                if (cols == null) {
                    queueOutput("," + counts[0] + "," + counts[1]);
                    return;
                }

                StringBuilder data = new StringBuilder();
                for (String col : cols) { data.append(",").append(col); }
                queueOutput("," + counts[0] + "," + counts[1] + data);

                for (String[] row : rows) {
                    data = new StringBuilder();
                    for (String val : row) { data.append(",").append(val); }
                    queueOutput(data.toString());
                }

            } catch (SQLTimeoutException e1) {
                queueError("Query read timeout");
            } catch (Exception e2) {
                queueError(e2);
            }
        }
    }
}
