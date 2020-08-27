/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.standard;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.dbcp.DBCPValidator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.sql.SqlWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.db.JdbcCommon;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import javax.security.auth.login.LoginException;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.nifi.processors.standard.sql.DriverShim;


public abstract class AbstractExecutePatternDbSQL extends AbstractProcessor {

    public static final String RESULT_ROW_COUNT = "executesql.row.count";
    public static final String RESULT_QUERY_DURATION = "executesql.query.duration";
    public static final String RESULT_QUERY_EXECUTION_TIME = "executesql.query.executiontime";
    public static final String RESULT_QUERY_FETCH_TIME = "executesql.query.fetchtime";
    public static final String RESULTSET_INDEX = "executesql.resultset.index";
    public static final String RESULT_ERROR_MESSAGE = "executesql.error.message";
    public static final String INPUT_FLOWFILE_UUID = "input.flowfile.uuid";

    public static final String SQL_PARAMS_DEFAULT="Unicode=true&characterEncoding=UTF-8&tinyInt1isBit=false&zeroDateTimeBehavior=convertToNull&useCursorFetch=true&defaultFetchSize=100";

    public static final String FRAGMENT_ID = FragmentAttributes.FRAGMENT_ID.key();
    public static final String FRAGMENT_INDEX = FragmentAttributes.FRAGMENT_INDEX.key();
    public static final String FRAGMENT_COUNT = FragmentAttributes.FRAGMENT_COUNT.key();


    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MIN_IDLE} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_MIN_IDLE = "0";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MAX_IDLE} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_MAX_IDLE = "8";
    /**
     * Copied from private variable {@link BasicDataSource.maxConnLifetimeMillis} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_EVICTION_RUN_PERIOD = String.valueOf(-1L);
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.7.0
     * and converted from 1800000L to "1800000 millis" to "30 mins"
     */
    private static final String DEFAULT_MIN_EVICTABLE_IDLE_TIME = "30 mins";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.7.0
     */
    private static final String DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME = String.valueOf(-1L);

    public static final PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
            .name("hosts")
            .displayName("Hosts")
            .description("A list of hostname/port entries corresponding to nodes in a MySQL cluster. The entries should be comma separated "
                    + "using a colon such as host1:port,host2:port,....  For example mysql.myhost.com:3306. This processor will attempt to connect to "
                    + "the hosts in the list in order. If one node goes down and failover is enabled for the cluster, then the processor will connect "
                    + "to the active node (assuming its host entry is specified in this property.  The default port for MySQL connections is 3306.")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("database-name")
            .displayName("Database Name")
            .description("Database Name. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor DB_URL_PARAMS = new PropertyDescriptor.Builder()
            .name("database-url-params")
            .displayName("Database Url Params")
            .description("Url Params. ")
            .required(false)
            .defaultValue(SQL_PARAMS_DEFAULT)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
            .name("Database Driver Class Name")
            .description("Database driver class name")
            .defaultValue(null)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("database-driver-locations")
            .displayName("Database Driver Location(s)")
            .description("Comma-separated list of files/folders and/or URLs containing the driver JAR and its dependencies (if any). For example '/var/tmp/mariadb-java-client-1.1.7.jar'")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.createURLorFileValidator()))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("Database User")
            .description("Database user name")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password for the database user")
            .defaultValue(null)
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time that the pool will wait (when there are no available connections) "
                    + " for a connection to be returned before failing, or -1 to wait indefinitely. ")
            .defaultValue("500 millis")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Max Total Connections")
            .description("The maximum number of active connections that can be allocated from this pool at the same time, "
                    + " or negative for no limit.")
            .defaultValue("8")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor VALIDATION_QUERY = new PropertyDescriptor.Builder()
            .name("Validation-query")
            .displayName("Validation query")
            .description("Validation query used to validate connections before returning them. "
                    + "When connection is invalid, it get's dropped and new valid connection will be returned. "
                    + "Note!! Using validation might have some performance penalty.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
            .displayName("Minimum Idle Connections")
            .name("dbcp-min-idle-conns")
            .description("The minimum number of connections that can remain idle in the pool, without extra ones being " +
                    "created, or zero to create none.")
            .defaultValue(DEFAULT_MIN_IDLE)
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_IDLE = new PropertyDescriptor.Builder()
            .displayName("Max Idle Connections")
            .name("dbcp-max-idle-conns")
            .description("The maximum number of connections that can remain idle in the pool, without extra ones being " +
                    "released, or negative for no limit.")
            .defaultValue(DEFAULT_MAX_IDLE)
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
            .displayName("Max Connection Lifetime")
            .name("dbcp-max-conn-lifetime")
            .description("The maximum lifetime in milliseconds of a connection. After this time is exceeded the " +
                    "connection will fail the next activation, passivation or validation test. A value of zero or less " +
                    "means the connection has an infinite lifetime.")
            .defaultValue(DEFAULT_MAX_CONN_LIFETIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor EVICTION_RUN_PERIOD = new PropertyDescriptor.Builder()
            .displayName("Time Between Eviction Runs")
            .name("dbcp-time-between-eviction-runs")
            .description("The number of milliseconds to sleep between runs of the idle connection evictor thread. When " +
                    "non-positive, no idle connection evictor thread will be run.")
            .defaultValue(DEFAULT_EVICTION_RUN_PERIOD)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .displayName("Minimum Evictable Idle Time")
            .name("dbcp-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for eviction.")
            .defaultValue(DEFAULT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SOFT_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .displayName("Soft Minimum Evictable Idle Time")
            .name("dbcp-soft-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for " +
                    "eviction by the idle connection evictor, with the extra condition that at least a minimum number of" +
                    " idle connections remain in the pool. When the not-soft version of this option is set to a positive" +
                    " value, it is examined first by the idle connection evictor: when idle connections are visited by " +
                    "the evictor, idle time is first compared against it (without considering the number of idle " +
                    "connections in the pool) and then against this soft option, including the minimum idle connections " +
                    "constraint.")
            .defaultValue(DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
//            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();
    protected Set<Relationship> relationships;

/*    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();*/

    public static final PropertyDescriptor SQL_PRE_QUERY = new PropertyDescriptor.Builder()
            .name("sql-pre-query")
            .displayName("SQL Pre-Query")
            .description("A semicolon-delimited list of queries executed before the main SQL query is executed. " +
                    "For example, set session properties before main query. " +
                    "Results/outputs from these queries will be suppressed if there are no errors.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("SQL select query")
            .description("The SQL select query to execute. The query can be empty, a constant value, or built from attributes "
                    + "using Expression Language. If this property is specified, it will be used regardless of the content of "
                    + "incoming flowfiles. If this property is empty, the content of the incoming flow file is expected "
                    + "to contain a valid SQL select query, to be issued by the processor to the database. Note that Expression "
                    + "Language is not evaluated for flow file contents.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor SQL_POST_QUERY = new PropertyDescriptor.Builder()
            .name("sql-post-query")
            .displayName("SQL Post-Query")
            .description("A semicolon-delimited list of queries executed after the main SQL query is executed. " +
                    "Example like setting session properties after main query. " +
                    "Results/outputs from these queries will be suppressed if there are no errors.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL select query "
                    + " , zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("esql-max-rows")
            .displayName("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("esql-output-batch-size")
            .displayName("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all result set rows "
                    + "have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles "
                    + "to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will "
                    + "be committed, thus releasing the FlowFiles to the downstream relationship. NOTE: The fragment.count attribute will not be set on FlowFiles when this "
                    + "property is set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("esql-fetch-size")
            .displayName("Fetch Size")
            .description("The number of result rows to be fetched from the result set at a time. This is a hint to the database driver and may not be "
                    + "honored and/or exact. If the value specified is zero, then the hint is ignored.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    protected List<PropertyDescriptor> propDescriptors;

    protected ConcurrentHashMap<String,BasicDataSource> dataSourceServiceMap = new ConcurrentHashMap<>();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        // If the query is not set, then an incoming flow file is needed. Otherwise fail the initialization
        if (!context.getProperty(SQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
        //dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

    }
    /**
     * Shutdown pool, close all open connections.
     * If a principal is authenticated with a KDC, that principal is logged out.
     *
     * If a @{@link LoginException} occurs while attempting to log out the @{@link org.apache.nifi.security.krb.KerberosUser},
     * an attempt will still be made to shut down the pool and close open connections.
     *
     * @throws SQLException if there is an error while closing open connections
     * @throws LoginException if there is an error during the principal log out, and will only be thrown if there was
     * no exception while closing open connections
     */
    @OnDisabled
    @OnShutdown
    public void shutdown() throws SQLException {
        for(BasicDataSource dataSource : dataSourceServiceMap.values()) {
            try {
                dataSource.close();
            } finally {
                dataSource = null;
            }
        }
    }
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;
        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final List<FlowFile> resultSetFlowFiles = new ArrayList<>();

        final ComponentLog logger = getLogger();
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final Integer maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer outputBatchSizeField = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final int outputBatchSize = outputBatchSizeField == null ? 0 : outputBatchSizeField;
        final Integer fetchSize = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions().asInteger();
        final String drv = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions(fileToProcess).getValue();

        List<String> preQueries = getQueries(context.getProperty(SQL_PRE_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());
        List<String> postQueries = getQueries(context.getProperty(SQL_POST_QUERY).evaluateAttributeExpressions(fileToProcess).getValue());

        SqlWriter sqlWriter = configureSqlWriter(session, context, fileToProcess);

        String selectQuery;
        if (context.getProperty(SQL_SELECT_QUERY).isSet()) {
            selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        } else {
            // If the query is not set, then an incoming flow file is required, and expected to contain a valid SQL select query.
            // If there is no incoming connection, onTrigger will not be called as the processor will fail when scheduled.
            final StringBuilder queryContents = new StringBuilder();
            session.read(fileToProcess, in -> queryContents.append(IOUtils.toString(in, Charset.defaultCharset())));
            selectQuery = queryContents.toString();
        }
        final String hosts = context.getProperty(HOSTS).evaluateAttributeExpressions(fileToProcess).getValue();
        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(fileToProcess).getValue();
        String dsKey=String.format("dskey_%s",hosts.replaceAll(",","_"));
        if(drv.contains("postgresql")){
            dsKey=String.format("dskey_%s_%s",hosts.replaceAll(",","_"),databaseName);
        }

        BasicDataSource dataSource = dataSourceServiceMap.get(dsKey);
        try {
            if (dataSource == null) {
                dataSource = createBasicDataSource(context, fileToProcess);
                BasicDataSource curDataSource = dataSourceServiceMap.putIfAbsent(dsKey, dataSource);
                if (curDataSource != null) {
                    dataSource.close();
                    dataSource = curDataSource;
                }
            }
        }catch(Exception e){
            //If we had at least one result then it's OK to drop the original file, but if we had no results then
            //  pass the original flow file down the line to trigger downstream processors
            if (fileToProcess == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to create dataSource {} and execute SQL select query {} due to {}. No FlowFile to route to failure", new Object[]{dsKey,selectQuery, e});
            } else {

                logger.error("Unable to create dataSource {} and execute SQL select query {} due to {}; routing to failure",
                        new Object[]{dsKey,selectQuery, e});
                session.putAttribute(fileToProcess,RESULT_ERROR_MESSAGE,e.getMessage());
                session.transfer(fileToProcess, REL_FAILURE);
                return;
            }
        }

        int resultCount = 0;
        try (//final Connection con = dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes());
             final Connection con = getConnection(dataSource);
             final PreparedStatement st = con.prepareStatement(selectQuery)) {
            if (fetchSize != null && fetchSize > 0) {
                try {
                    st.setFetchSize(fetchSize);
                } catch (SQLException se) {
                    // Not all drivers support this, just log the error (at debug level) and move on
                    logger.debug("Cannot set fetch size to {} due to {}", new Object[]{fetchSize, se.getLocalizedMessage()}, se);
                }
            }
            st.setQueryTimeout(queryTimeout); // timeout in seconds

            // Execute pre-query, throw exception and cleanup Flow Files if fail
            Pair<String,SQLException> failure = executeConfigStatements(con, preQueries);
            if (failure != null) {
                // In case of failure, assigning config query to "selectQuery" to follow current error handling
                selectQuery = failure.getLeft();
                throw failure.getRight();
            }

            if (fileToProcess != null) {
                JdbcCommon.setParameters(st, fileToProcess.getAttributes());
            }
            logger.debug("Executing query {}", new Object[]{selectQuery});

            int fragmentIndex = 0;
            final String fragmentId = UUID.randomUUID().toString();

            final StopWatch executionTime = new StopWatch(true);

            boolean hasResults = st.execute();

            long executionTimeElapsed = executionTime.getElapsed(TimeUnit.MILLISECONDS);

            boolean hasUpdateCount = st.getUpdateCount() != -1;

            Map<String, String> inputFileAttrMap = fileToProcess == null ? null : fileToProcess.getAttributes();
            String inputFileUUID = fileToProcess == null ? null : fileToProcess.getAttribute(CoreAttributes.UUID.key());
            while (hasResults || hasUpdateCount) {
                //getMoreResults() and execute() return false to indicate that the result of the statement is just a number and not a ResultSet
                if (hasResults) {
                    final AtomicLong nrOfRows = new AtomicLong(0L);

                    try {
                        final ResultSet resultSet = st.getResultSet();
                        do {
                            final StopWatch fetchTime = new StopWatch(true);

                            FlowFile resultSetFF;
                            if (fileToProcess == null) {
                                resultSetFF = session.create();
                            } else {
                                resultSetFF = session.create(fileToProcess);
                            }

                            if (inputFileAttrMap != null) {
                                resultSetFF = session.putAllAttributes(resultSetFF, inputFileAttrMap);
                            }


                            try {
                                resultSetFF = session.write(resultSetFF, out -> {
                                    try {
                                        nrOfRows.set(sqlWriter.writeResultSet(resultSet, out, getLogger(), null));
                                    } catch (Exception e) {
                                        throw (e instanceof ProcessException) ? (ProcessException) e : new ProcessException(e);
                                    }
                                });

                                long fetchTimeElapsed = fetchTime.getElapsed(TimeUnit.MILLISECONDS);

                                // set attributes
                                final Map<String, String> attributesToAdd = new HashMap<>();
                                attributesToAdd.put(RESULT_ROW_COUNT, String.valueOf(nrOfRows.get()));
                                attributesToAdd.put(RESULT_QUERY_DURATION, String.valueOf(executionTimeElapsed + fetchTimeElapsed));
                                attributesToAdd.put(RESULT_QUERY_EXECUTION_TIME, String.valueOf(executionTimeElapsed));
                                attributesToAdd.put(RESULT_QUERY_FETCH_TIME, String.valueOf(fetchTimeElapsed));
                                attributesToAdd.put(RESULTSET_INDEX, String.valueOf(resultCount));
                                if (inputFileUUID != null) {
                                    attributesToAdd.put(INPUT_FLOWFILE_UUID, inputFileUUID);
                                }
                                attributesToAdd.putAll(sqlWriter.getAttributesToAdd());
                                resultSetFF = session.putAllAttributes(resultSetFF, attributesToAdd);
                                sqlWriter.updateCounters(session);

                                // if fragmented ResultSet, determine if we should keep this fragment; set fragment attributes
                                if (maxRowsPerFlowFile > 0) {
                                    // if row count is zero and this is not the first fragment, drop it instead of committing it.
                                    if (nrOfRows.get() == 0 && fragmentIndex > 0) {
                                        session.remove(resultSetFF);
                                        break;
                                    }

                                    resultSetFF = session.putAttribute(resultSetFF, FRAGMENT_ID, fragmentId);
                                    resultSetFF = session.putAttribute(resultSetFF, FRAGMENT_INDEX, String.valueOf(fragmentIndex));
                                }

                                logger.info("{} contains {} records; transferring to 'success'",
                                        new Object[]{resultSetFF, nrOfRows.get()});
                                // Report a FETCH event if there was an incoming flow file, or a RECEIVE event otherwise
                                if(context.hasIncomingConnection()) {
                                    session.getProvenanceReporter().fetch(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
                                } else {
                                    session.getProvenanceReporter().receive(resultSetFF, "Retrieved " + nrOfRows.get() + " rows", executionTimeElapsed + fetchTimeElapsed);
                                }
                                resultSetFlowFiles.add(resultSetFF);

                                // If we've reached the batch size, send out the flow files
                                if (outputBatchSize > 0 && resultSetFlowFiles.size() >= outputBatchSize) {
                                    session.transfer(resultSetFlowFiles, REL_SUCCESS);
                                    // Need to remove the original input file if it exists
                                    if (fileToProcess != null) {
                                        session.remove(fileToProcess);
                                        fileToProcess = null;
                                    }
                                    session.commit();
                                    resultSetFlowFiles.clear();
                                }

                                fragmentIndex++;
                            } catch (Exception e) {
                                // Remove the result set flow file and propagate the exception
                                session.remove(resultSetFF);
                                if (e instanceof ProcessException) {
                                    throw (ProcessException) e;
                                } else {
                                    throw new ProcessException(e);
                                }
                            }
                        } while (maxRowsPerFlowFile > 0 && nrOfRows.get() == maxRowsPerFlowFile);

                        // If we are splitting results but not outputting batches, set count on all FlowFiles
                        if (outputBatchSize == 0 && maxRowsPerFlowFile > 0) {
                            for (int i = 0; i < resultSetFlowFiles.size(); i++) {
                                resultSetFlowFiles.set(i,
                                        session.putAttribute(resultSetFlowFiles.get(i), FRAGMENT_COUNT, Integer.toString(fragmentIndex)));
                            }
                        }
                    } catch (final SQLException e) {
                        throw new ProcessException(e);
                    }

                    resultCount++;
                }

                // are there anymore result sets?
                try {
                    hasResults = st.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
                    hasUpdateCount = st.getUpdateCount() != -1;
                } catch (SQLException ex) {
                    hasResults = false;
                    hasUpdateCount = false;
                }
            }

            // Execute post-query, throw exception and cleanup Flow Files if fail
            failure = executeConfigStatements(con, postQueries);
            if (failure != null) {
                selectQuery = failure.getLeft();
                resultSetFlowFiles.forEach(ff -> session.remove(ff));
                throw failure.getRight();
            }

            // Transfer any remaining files to SUCCESS
            session.transfer(resultSetFlowFiles, REL_SUCCESS);
            resultSetFlowFiles.clear();

            //If we had at least one result then it's OK to drop the original file, but if we had no results then
            //  pass the original flow file down the line to trigger downstream processors
            if (fileToProcess != null) {
                if (resultCount > 0) {
                    session.remove(fileToProcess);
                } else {
                    fileToProcess = session.write(fileToProcess, out -> sqlWriter.writeEmptyResultSet(out, getLogger()));
                    fileToProcess = session.putAttribute(fileToProcess, RESULT_ROW_COUNT, "0");
                    fileToProcess = session.putAttribute(fileToProcess, CoreAttributes.MIME_TYPE.key(), sqlWriter.getMimeType());
                    session.transfer(fileToProcess, REL_SUCCESS);
                }
            } else if (resultCount == 0) {
                //If we had no inbound FlowFile, no exceptions, and the SQL generated no result sets (Insert/Update/Delete statements only)
                // Then generate an empty Output FlowFile
                FlowFile resultSetFF = session.create();

                resultSetFF = session.write(resultSetFF, out -> sqlWriter.writeEmptyResultSet(out, getLogger()));
                resultSetFF = session.putAttribute(resultSetFF, RESULT_ROW_COUNT, "0");
                resultSetFF = session.putAttribute(resultSetFF, CoreAttributes.MIME_TYPE.key(), sqlWriter.getMimeType());
                session.transfer(resultSetFF, REL_SUCCESS);
            }
        } catch (final ProcessException | SQLException e) {
            //If we had at least one result then it's OK to drop the original file, but if we had no results then
            //  pass the original flow file down the line to trigger downstream processors
            if (fileToProcess == null) {
                // This can happen if any exceptions occur while setting up the connection, statement, etc.
                logger.error("Unable to execute SQL select query {} due to {}. No FlowFile to route to failure",
                        new Object[]{selectQuery, e});
                context.yield();
            } else {
                if (context.hasIncomingConnection()) {
                    logger.error("Unable to execute SQL select query {} for {} due to {}; routing to failure",
                            new Object[]{selectQuery, fileToProcess, e});
                    fileToProcess = session.penalize(fileToProcess);
                } else {
                    logger.error("Unable to execute SQL select query {} due to {}; routing to failure",
                            new Object[]{selectQuery, e});
                    context.yield();
                }
                session.putAttribute(fileToProcess,RESULT_ERROR_MESSAGE,e.getMessage());
                session.transfer(fileToProcess, REL_FAILURE);
            }
        }
    }

    /*
     * Executes given queries using pre-defined connection.
     * Returns null on success, or a query string if failed.
     */
    protected Pair<String,SQLException> executeConfigStatements(final Connection con, final List<String> configQueries){
        if (configQueries == null || configQueries.isEmpty()) {
            return null;
        }

        for (String confSQL : configQueries) {
            try(final Statement st = con.createStatement()){
                st.execute(confSQL);
            } catch (SQLException e) {
                return Pair.of(confSQL, e);
            }
        }
        return null;
    }

    /*
     * Extract list of queries from config property
     */
    protected List<String> getQueries(final String value) {
        if (value == null || value.length() == 0 || value.trim().length() == 0) {
            return null;
        }
        final List<String> queries = new LinkedList<>();
        for (String query : value.split(";")) {
            if (query.trim().length() > 0) {
                queries.add(query.trim());
            }
        }
        return queries;
    }

    protected abstract SqlWriter configureSqlWriter(ProcessSession session, ProcessContext context, FlowFile fileToProcess);

    protected BasicDataSource createBasicDataSource(ProcessContext context, FlowFile fileToProcess) throws InitializationException{

        final String drv = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions(fileToProcess).getValue();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions(fileToProcess).getValue();
        final String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions(fileToProcess).getValue();
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions(fileToProcess).asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        final Long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions(fileToProcess));
        final Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Integer maxIdle = context.getProperty(MAX_IDLE).evaluateAttributeExpressions(fileToProcess).asInteger();
        final Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions(fileToProcess));
        final Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD).evaluateAttributeExpressions(fileToProcess));
        final Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions(fileToProcess));
        final Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions(fileToProcess));

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(drv);

        // Optional driver URL, when exist, this URL will be used to locate driver jar file location
        final String urlString = context.getProperty(DB_DRIVER_LOCATION).evaluateAttributeExpressions(fileToProcess).getValue();
        dataSource.setDriverClassLoader(getDriverClassLoader(urlString, drv));

        final String hosts = context.getProperty(HOSTS).evaluateAttributeExpressions(fileToProcess).getValue();
        final String databaseName = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions(fileToProcess).getValue();
        final String dbUrlParams = context.getProperty(DB_URL_PARAMS).evaluateAttributeExpressions(fileToProcess).getValue();
        String dbType = "mysql";
        if(drv.contains("postgresql")){
            dbType="postgresql";
        }

        final String dburl =String.format("jdbc:%s://%s/%s?%s",dbType,hosts,databaseName,dbUrlParams);

        dataSource.setMaxWaitMillis(maxWaitMillis);
        dataSource.setMaxTotal(maxTotal);
        dataSource.setMinIdle(minIdle);
        dataSource.setMaxIdle(maxIdle);
        dataSource.setMaxConnLifetimeMillis(maxConnLifetimeMillis);
        dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        dataSource.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);

        if (validationQuery!=null && !validationQuery.isEmpty()) {
            dataSource.setValidationQuery(validationQuery);
            dataSource.setTestOnBorrow(true);
        }

        dataSource.setUrl(dburl);
        dataSource.setUsername(user);
        dataSource.setPassword(passw);

        context.getProperties().keySet().stream().filter(PropertyDescriptor::isDynamic)
                .forEach((dynamicPropDescriptor) -> dataSource.addConnectionProperty(dynamicPropDescriptor.getName(),
                        context.getProperty(dynamicPropDescriptor).evaluateAttributeExpressions().getValue()));
        return dataSource;
    }

    /**
     * using Thread.currentThread().getContextClassLoader(); will ensure that you are using the ClassLoader for you NAR.
     *
     * @throws InitializationException
     *             if there is a problem obtaining the ClassLoader
     */
    protected ClassLoader getDriverClassLoader(String locationString, String drvName) throws InitializationException {
        if (locationString != null && locationString.length() > 0) {
            try {
                // Split and trim the entries
                final ClassLoader classLoader = ClassLoaderUtils.getCustomClassLoader(
                        locationString,
                        this.getClass().getClassLoader(),
                        (dir, name) -> name != null && name.endsWith(".jar")
                );

                // Workaround which allows to use URLClassLoader for JDBC driver loading.
                // (Because the DriverManager will refuse to use a driver not loaded by the system ClassLoader.)
                final Class<?> clazz = Class.forName(drvName, true, classLoader);
                if (clazz == null) {
                    throw new InitializationException("Can't load Database Driver " + drvName);
                }
                final Driver driver = (Driver) clazz.newInstance();
                DriverManager.registerDriver(new DriverShim(driver));

                return classLoader;
            } catch (final MalformedURLException e) {
                throw new InitializationException("Invalid Database Driver Jar Url", e);
            } catch (final Exception e) {
                throw new InitializationException("Can't load Database Driver", e);
            }
        } else {
            // That will ensure that you are using the ClassLoader for you NAR.
            return Thread.currentThread().getContextClassLoader();
        }
    }
    private Long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? -1 : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }

    public Connection getConnection(BasicDataSource dataSource) throws ProcessException {
        try {
            final Connection con;
            con = dataSource.getConnection();
            return con;
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

}
