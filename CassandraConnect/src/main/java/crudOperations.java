import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.UUID;

public class crudOperations {
    public CqlSession session;
    private static String TABLE_NAME_BOOKS = "books";
    private static String TABLE_NAME_BOOKSTITLE = "booksByTitle";
    public crudOperations(CqlSession session){
        this.session=session;
    }
    public void createKeyspace(
            String keyspaceName, String replicationStrategy, int replicationFactor) {
        StringBuilder sb =
                new StringBuilder("CREATE KEYSPACE IF NOT EXISTS ")
                        .append(keyspaceName).append(" WITH replication = {")
                        .append("'class':'").append(replicationStrategy)
                        .append("','replication_factor':").append(replicationFactor)
                        .append("};");

        String query = sb.toString();
        ResultSet rs =session.execute(query);
    }
    public void createTableBooks(String keyspace) {
        String keysp = keyspace;
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(keysp).append(".")
                .append(TABLE_NAME_BOOKS).append("(")
                .append("id uuid PRIMARY KEY, ")
                .append("title text,")
                .append("subject text);");

        String query = sb.toString();
        session.execute(query);

    }
    public void alterTableBooks(String keyspace, String columnName, String columnType) {
        String keysp = keyspace;
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(keysp).append(".")
                .append(TABLE_NAME_BOOKS).append(" ADD ")
                .append(columnName).append(" ")
                .append(columnType).append(";");
        String query = sb.toString();
        session.execute(query);
    }
    public void insertRowBooks(String keyspace, String title, String subject, String publisher){
        String keysp = keyspace;
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(keysp).append(".")
                .append(TABLE_NAME_BOOKS).append("(id, title, subject, publisher) ")
                .append("VALUES (").append(UUID.randomUUID())
                .append(", '").append(title).append("'")
                .append(", '").append(subject).append("'")
                .append(", '").append(publisher).append("');");

        String query = sb.toString();
        ResultSet result = session.execute(query);
    }

    public void queryBooks(String keyspace) {

        ResultSet result = session.execute("SELECT * FROM " + keyspace + "." + TABLE_NAME_BOOKS);

        for (Row row : result) {
            String id = row.getUuid("id").toString();
            String title = row.getString("title");
            String subject = row.getString("subject");
            String publisher = row.getString("publisher");
            System.out.println("ID:"+ id + " " + "Title:" + title + " " + "Subject:" + subject + " " + "Publisher:" + publisher);
        }

    }

    public void queryBooksTitle(String keyspace) {

        ResultSet result = session.execute("SELECT * FROM " + keyspace + "." + TABLE_NAME_BOOKSTITLE);

        for (Row row : result) {
            String id = row.getUuid("id").toString();
            String title = row.getString("title");
            System.out.println("ID:"+ id + " " + "Title:" + title + " " );
        }

    }

    public void createTableBooksTitle(String keyspace) {
        String keysp = keyspace;
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(keysp).append(".")
                .append(TABLE_NAME_BOOKSTITLE).append("(")
                .append("id uuid, ")
                .append("title text,")
                .append("PRIMARY KEY (title, id));");
        String query = sb.toString();
        session.execute(query);

    }

    public void insertBookBatch(String keyspace, String title, String subject, String publisher) {
        UUID id = UUID.randomUUID();
        StringBuilder sb = new StringBuilder("BEGIN BATCH ")
                .append("INSERT INTO ").append(keyspace).append(".").append(TABLE_NAME_BOOKS)
                .append("(id, title, subject) ")
                .append("VALUES (").append(id).append(", '")
                .append(title).append("', '")
                .append(subject).append("');")
                .append("INSERT INTO ")
                .append(keyspace).append(".")
                .append(TABLE_NAME_BOOKSTITLE).append("(id, title) ")
                .append("VALUES (").append(id).append(", '")
                .append(title).append("');")
                .append("APPLY BATCH;");

        String query = sb.toString();
        session.execute(query);
    }

    public void deleteTableBook(String keyspace) {
        StringBuilder sb =
                new StringBuilder("DROP TABLE IF EXISTS ").append(keyspace).append(".").append(TABLE_NAME_BOOKS);
        String query = sb.toString();
        session.execute(query);
    }

    public void deleteTableBookTitle(String keyspace) {
        StringBuilder sb =
                new StringBuilder("DROP TABLE IF EXISTS ").append(keyspace).append(".").append(TABLE_NAME_BOOKSTITLE);
        String query = sb.toString();
        session.execute(query);
    }

    public void deleteKeyspace(String keyspace) {
        StringBuilder sb =
                new StringBuilder("DROP KEYSPACE ").append(keyspace);
        String query = sb.toString();
        session.execute(query);
    }
}
