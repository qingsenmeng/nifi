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

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.Strings;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.sql.*;
import java.util.*;


@EventDriven
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"sql", "truncate", "jdbc", "database", "record"})
@CapabilityDescription("Executes provided SQL execute truncate or delete table.  ")
@Stateful(scopes = Scope.CLUSTER, description = "Information such as if table is truncated is stored by this processor, such "
        + "that it can continue from the same location if restarted.")
@WritesAttributes({

        @WritesAttribute(attribute = "executesql.query.duration", description = "Combined duration of the query execution time and fetch time in milliseconds"),
        @WritesAttribute(attribute = "executesql.query.executiontime", description = "Duration of the query execution time in milliseconds"),
        @WritesAttribute(attribute = "executesql.query.fetchtime", description = "Duration of the result set fetch time in milliseconds"),
        @WritesAttribute(attribute = "executesql.resultset.index", description = "Assuming multiple result sets are returned, "
                + "the zero based index of this result set."),
        @WritesAttribute(attribute = "executesql.error.message", description = "If processing an incoming flow file causes "
                + "an Exception, the Flow File is routed to failure and this attribute is set to the exception message."),
        @WritesAttribute(attribute = "fragment.identifier", description = "If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet. If Output Batch Size is set, then this "
                + "attribute will not be populated."),
        @WritesAttribute(attribute = "fragment.index", description = "If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced"),
        @WritesAttribute(attribute = "input.flowfile.uuid", description = "If the processor has an incoming connection, outgoing FlowFiles will have this attribute "
                + "set to the value of the input FlowFile's UUID. If there is no incoming connection, the attribute will not be added."),
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer."),
        @WritesAttribute(attribute = "record.count", description = "The number of records output by the Record Writer.")
})
public class ClearTableData extends AbstractProcessor {

    private static final String defaultDeleteSqlPattern = "truncate table ${tableName}";

    public static final String RESULT_ERROR_MESSAGE = "executesql.error.message";

    public static final String IS_TRUNCATED_TABLE = "is_truncated_table";

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
            .build();
    public static final String TABLE_NAME_KEY = "tableName";
    protected Set<Relationship> relationships;

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor SQL_DELETE = new PropertyDescriptor.Builder()
            .name("SQL delete table")
            .description("The SQL delete table data to execute.")
            .required(true)
            .defaultValue(defaultDeleteSqlPattern)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor TABLE_NAME = new PropertyDescriptor.Builder()
            .name("Table name")
            .description("The SQL delete table name.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor IS_TRUNCATE_TABLE = new PropertyDescriptor.Builder()
            .name("Is delete table flag")
            .description("If is flag is true, delete table data SQL will execute.")
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    protected List<PropertyDescriptor> propDescriptors;

    protected DBCPService dbcpService;

    public ClearTableData() {
        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        r.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(DBCP_SERVICE);
        pds.add(SQL_DELETE);
        pds.add(TABLE_NAME);
        pds.add(IS_TRUNCATE_TABLE);
        propDescriptors = Collections.unmodifiableList(pds);
    }
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
        if (!context.getProperty(SQL_DELETE).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Delete SQL must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL Delete Statement";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

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

        final ComponentLog logger = getLogger();
        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;

        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            logger.error("Failed to retrieve observed maximum values from the State Manager. Will not attempt "
                    + "connection until this is accomplished.", ioe);
            context.yield();
            return;
        }

        String tableName = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(fileToProcess).getValue();
        String clearTableDataFlag = stateMap.get(tableName+"."+IS_TRUNCATED_TABLE);
        final String isTruncateTable = context.getProperty(IS_TRUNCATE_TABLE).evaluateAttributeExpressions().getValue();

        if((StringUtils.isEmpty(clearTableDataFlag) || !Boolean.valueOf(clearTableDataFlag)) &&  !StringUtils.isEmpty(isTruncateTable) && Boolean.valueOf(isTruncateTable)) {
            Map<String, String> generatedAttributes = new HashMap<String, String>();
            generatedAttributes.put(TABLE_NAME_KEY, tableName);
            String selectQuery = Strings.replaceVariables(context.getProperty(SQL_DELETE).getValue(), generatedAttributes::get);

            try (final Connection con = dbcpService.getConnection(fileToProcess == null ? Collections.emptyMap() : fileToProcess.getAttributes());
                 final PreparedStatement st = con.prepareStatement(selectQuery)) {
                st.execute();
                updateState(stateManager, tableName,"true");
            } catch (final ProcessException | SQLException | IOException e) {
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
                    session.putAttribute(fileToProcess, RESULT_ERROR_MESSAGE, e.getMessage());
                    session.transfer(fileToProcess, REL_FAILURE);
                }
            }
        }
        session.transfer(fileToProcess, REL_SUCCESS);
    }
    private void updateState(StateManager stateManager,String tableName, String flag) throws IOException {
        // Update state with latest values
        if (stateManager != null) {
            Map<String, String> newStateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());

            // Save current binlog filename and position to the state map
            newStateMap.put(tableName+"."+IS_TRUNCATED_TABLE, flag);
            stateManager.setState(newStateMap, Scope.CLUSTER);
        }
    }
}
