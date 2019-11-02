import java.net.URL;
import oracle.nosql.driver.*;
import oracle.nosql.driver.idcs.*;
import oracle.nosql.driver.ops.*;
import oracle.nosql.driver.values.MapValue;

public class HelloWorld {

    /* Name of your table */
    private static final String tableName = "HelloWorldTable";

    public static void main(String[] args) throws Exception {

        /* Set up an endpoint URL */
        URL serviceURL = new URL("https", getEndpoint(args), 443, "/");
        System.out.println("Using endpoint: " + serviceURL);

        /*
         * Fill in the MyCredentials class below and use it to 
         * create an access token provider. This lets your 
         * application perform authentication and authorization 
	 * operations to the cloud service.
         */
        DefaultAccessTokenProvider provider =
            new DefaultAccessTokenProvider(MyCredentials.ENTITLEMENT_ID,
                                           MyCredentials.IDCS_URL,
                                           false,
                                           new MyCredentials(), 
                                           300000, 
                                           85400);

        /* Create a NoSQL handle to access the cloud service */
        NoSQLHandleConfig config = new NoSQLHandleConfig(serviceURL);
        config.setAuthorizationProvider(provider);
        NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config);

        /* Create a table and run operations. Be sure to close the handle */
        try {
            if (isDrop(args))
                dropTable(handle); // -drop was specified
            else
                helloWorld(handle);
        } finally {
            handle.close();
        }
    }

    /**
     * Create a table and do some operations.
     */
    private static void helloWorld(NoSQLHandle handle) throws Exception {

        /*
         * Create a simple table with an integer key and a single 
         * string data field and set your desired table capacity.
         */
        String createTableDDL = "CREATE TABLE IF NOT EXISTS " + tableName +
                                "(id INTEGER, name STRING, " +
                                "PRIMARY KEY(id))";

        TableLimits limits = new TableLimits(1, 2, 1);
        TableRequest treq = new TableRequest().setStatement(createTableDDL).
            setTableLimits(limits);
        System.out.println("Creating table " + tableName);
        TableResult tres = handle.tableRequest(treq);

        /* The request is async, so wait for the table to become active.*/
        System.out.println("Waiting for " + tableName + " to become active");
        TableResult.waitForState(handle,tres.getTableName(),
                TableResult.State.ACTIVE, 60000, 1000);
        System.out.println("Table " + tableName + " is active");

        /* Make a row and write it */
        MapValue value = new MapValue().put("id", 29).put("name", "Tracy");
        PutRequest putRequest = new PutRequest().setValue(value)
            .setTableName(tableName);

        handle.put(putRequest);
        System.out.println("Wrote " + value);

        /* Make a key and read the row */
        MapValue key = new MapValue().put("id", 29);
        GetRequest getRequest = new GetRequest().setKey(key)
            .setTableName(tableName);

        GetResult getRes = handle.get(getRequest);
        System.out.println("Read " + getRes.getValue());

        /* At this point, you can see your table in the Identity Console */
    }

    /** Remove the table. */
    private static void dropTable(NoSQLHandle handle) throws Exception {

        /* Drop the table and wait for the table to move to dropped state */
        System.out.println("Dropping table " + tableName);
        TableRequest treq = new TableRequest().setStatement
            ("DROP TABLE IF EXISTS " + tableName);
        TableResult tres = handle.tableRequest(treq);

        System.out.println("Waiting for " + tableName + " to be dropped");
        TableResult.waitForState(handle,tres.getTableName(),
                TableResult.State.DROPPED, 100000, 1000);
        System.out.println("Table " + tableName + " has been dropped");
    }

    /** Get the end point from the arguments */
    private static String getEndpoint(String[] args) {
        if (args.length > 0)
            return args[0];

        System.err.println
            ("Usage: java -cp .:oracle-nosql-cloud-java-driver-X.Y/lib/* " +
             " HelloWorld <endpoint> [-drop]\n");
        System.exit(1);
        return null;
    }

    /** Return true if -drop is specified */
    private static boolean isDrop(String[] args) {
        if (args.length < 2)
            return false;
        return args[1].equalsIgnoreCase("-drop");
    }

    /**
     * Use this simple implementation of the CredentialsProvider 
     * interface to pass the your credentials to HelloWorld. 
     * Replace the appropriate parts of MyCredentials 
     * before trying the example.
     */
    private static class MyCredentials implements CredentialsProvider {

        static final String ENTITLEMENT_ID =
            "EDIT: put your IDCS entitlement ID here";
        static final String IDCS_URL = "EDIT: put your IDCS URL here";

        private String refreshToken;

        @Override
        public IDCSCredentials getOAuthClientCredentials() {
            return new IDCSCredentials
                ("EDIT: put your IDCS client ID here",
                 "EDIT: put your IDCS client secret here"
                 .toCharArray());
        }

        @Override
        public IDCSCredentials getUserCredentials() {
            return new IDCSCredentials
                ("EDIT: put your Oracle Cloud user name here",
                 "EDIT: put your Oracle Cloud password here"
                 .toCharArray());
        }

        @Override
        public void storeServiceRefreshToken(String token) {
            refreshToken = token;
        }

        @Override
        public String getServiceRefreshToken() {
            return(refreshToken);
        }
    }
}
