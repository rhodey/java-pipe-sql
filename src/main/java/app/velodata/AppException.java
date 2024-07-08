package app.velodata;

public class AppException extends Exception {

    private final String message;
    private final Integer connection;
    private final String query;

    private static String getMessage(Exception e) {
        if (e.getMessage() == null) {
            return e.getClass().getName();
        } else {
            return e.getMessage();
        }
    }

    public AppException(Integer connection, String query, String message) {
        this.message = message;
        this.connection = connection;
        this.query = query;
    }

    public AppException(Integer connection, String query, Exception e) {
        this.message = getMessage(e);
        if (e instanceof AppException) {
            this.connection = ((AppException) e).getConnection();
            this.query = ((AppException) e).getQuery();
        } else {
            this.connection = connection;
            this.query = query;
        }
    }

    public AppException(Integer connection, String message) {
        this(connection, null, message);
    }

    public AppException(Integer connection, Exception e) {
        this(connection, null, e);
    }

    public AppException(String message) {
        this(null, null, message);
    }

    public AppException(Exception e) {
        this(null, null, e);
    }

    @Override
    public String getMessage() {
        return this.message;
    }

    public Integer getConnection() {
        return connection;
    }

    public String getQuery() {
        return query;
    }
}
