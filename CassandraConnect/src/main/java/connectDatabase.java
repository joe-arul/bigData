import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.nio.file.Paths;


public class connectDatabase {

    public static void main(String[] args) {

// Create session object by establishing database connection
        CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(Paths.get("C:\\Users\\joear\\Downloads\\secure-connect-jasper.zip"))
                .withAuthCredentials("dUZOrcXZshZLZDLYpUnvtFgx","3Id+pYAm7qn.rAy_e4Y6aDWozhROiePTOl,s5gCYnfW5v2dM2iwZ-Dv,pIM_yTECRBOJM.mJyl8k9q3GRk52QKPq1QQlZxUqCZ.YXNcekZ6ji5Ss+aI4hu-TZhu-g0_j")
                .build();

// Create object for crudOperations class
        crudOperations crud = new crudOperations(session) ;

// 1) Create a schema in existing jasper db
        String keyspaceName = "library";
        //crud.createKeyspace(keyspaceName, "SimpleStrategy", 1);

// 2) Create 'books' table
        //crud.createTableBooks(keyspaceName);

// 3) Alter table 'books'
        //crud.alterTableBooks(keyspaceName,"publisher","text");

// 4) Insert data into 'books'

        //crud.insertRowBooks(keyspaceName,"first title","first subject","first publisher");
        //crud.insertRowBooks(keyspaceName,"second title","second subject", "second publisher");

// 5) Querying from 'books' table
        //crud.queryBooks(keyspaceName);

// 6) Create 'booksByTitle' table
        //crud.createTableBooksTitle(keyspaceName);

// 7) Insert data to both tables with batch query
        //crud.insertBookBatch(keyspaceName,"third title", "third subject","third publisher");
        //crud.queryBooks(keyspaceName);
        //crud.queryBooksTitle(keyspaceName);

// 8) Delete table
        //crud.deleteTableBook(keyspaceName);
        //crud.deleteTableBookTitle(keyspaceName);

// 9) Delete keyspace
        //crud.deleteKeyspace(keyspaceName);

        session.close();

    }


}