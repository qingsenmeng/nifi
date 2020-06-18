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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.util.db.SimpleCommerceDataSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestExecutePatternDbSQLRecord {

    private static final Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.ExecuteSQLRecord", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestExecuteSQLRecord", "debug");
        LOGGER = LoggerFactory.getLogger(TestExecutePatternDbSQLRecord.class);
    }

    final static String DB_LOCATION = "target/db";

    final static String QUERY_WITH_EL = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID = ${person.id}";

    final static String QUERY_WITHOUT_EL = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID = 10";

    final static String QUERY_WITHOUT_EL_WITH_PARAMS = "select "
            + "  PER.ID as PersonId, PER.NAME as PersonName, PER.CODE as PersonCode"
            + ", PRD.ID as ProductId,PRD.NAME as ProductName,PRD.CODE as ProductCode"
            + ", REL.ID as RelId,    REL.NAME as RelName,    REL.CODE as RelCode"
            + ", ROW_NUMBER() OVER () as rownr "
            + " from persons PER, products PRD, relationships REL"
            + " where PER.ID < ? AND REL.ID < ?";


    @BeforeClass
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl("derby");
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ExecutePatternDbSQLRecord.class);
        /*runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(AbstractExecuteSQL.DBCP_SERVICE, "dbcp");*/
        runner.setValidateExpressionUsage(false);
    }

    @Test
    public void testIncomingConnectionWithNoFlowFile() throws InitializationException {
        runner.setIncomingConnection(true);
        runner.setProperty(AbstractExecuteSQL.SQL_SELECT_QUERY, "SELECT * FROM persons");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecuteSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();
        runner.assertTransferCount(AbstractExecuteSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractExecuteSQL.REL_FAILURE, 0);
    }

    @Test
    public void testIncomingConnectionWithNoFlowFileAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(true);
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecutePatternDbSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();
        runner.assertTransferCount(AbstractExecutePatternDbSQL.REL_SUCCESS, 0);
        runner.assertTransferCount(AbstractExecutePatternDbSQL.REL_FAILURE, 0);
    }

    @Test(expected = AssertionError.class)
    public void testNoIncomingConnectionAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(false);
        runner.run();
    }

    @Test
    public void testNoIncomingConnection() throws ClassNotFoundException, SQLException, InitializationException, IOException {
        runner.setIncomingConnection(false);
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, false, null, true);
        assertEquals(ProvenanceEventType.RECEIVE, runner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    public void testSelectQueryInFlowFile() throws InitializationException, ClassNotFoundException, SQLException, IOException {
        invokeOnTriggerRecords(null, QUERY_WITHOUT_EL, true, null, false);
        assertEquals(ProvenanceEventType.FORK, runner.getProvenanceEvents().get(0).getEventType());
        assertEquals(ProvenanceEventType.FETCH, runner.getProvenanceEvents().get(1).getEventType());
    }

    @Test
    public void testWithOutputBatching() throws InitializationException, SQLException {
        // remove previous test database, if any

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecutePatternDbSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(false);
        runner.setProperty(ExecutePatternDbSQLRecord.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(ExecutePatternDbSQLRecord.OUTPUT_BATCH_SIZE, "5");
        runner.setProperty(ExecutePatternDbSQLRecord.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecutePatternDbSQLRecord.REL_SUCCESS, 200);
        runner.assertAllFlowFilesContainAttribute(ExecutePatternDbSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecutePatternDbSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecutePatternDbSQLRecord.REL_SUCCESS).get(0);

        firstFlowFile.assertAttributeEquals(ExecutePatternDbSQLRecord.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecutePatternDbSQLRecord.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecutePatternDbSQLRecord.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(ExecutePatternDbSQLRecord.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(ExecutePatternDbSQLRecord.RESULTSET_INDEX, "0");
    }

    @Test
    public void testWithOutputBatchingAndIncomingFlowFile() throws InitializationException, SQLException {
        // remove previous test database, if any
        Map<String, String> attrMap = new HashMap<>();
        String testAttrName = "attr1";
        String testAttrValue = "value1";
        attrMap.put(testAttrName, testAttrValue);

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecutePatternDbSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        runner.setIncomingConnection(true);
        runner.setProperty(ExecutePatternDbSQLRecord.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(ExecutePatternDbSQLRecord.OUTPUT_BATCH_SIZE, "1");
        MockFlowFile inputFlowFile = runner.enqueue("SELECT * FROM TEST_NULL_INT", attrMap);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExecutePatternDbSQLRecord.REL_SUCCESS, 200);
        runner.assertAllFlowFilesContainAttribute(ExecutePatternDbSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(ExecutePatternDbSQLRecord.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(ExecutePatternDbSQLRecord.REL_SUCCESS).get(0);

        firstFlowFile.assertAttributeEquals(ExecutePatternDbSQLRecord.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeNotExists(FragmentAttributes.FRAGMENT_COUNT.key());
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(ExecutePatternDbSQLRecord.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(ExecutePatternDbSQLRecord.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(ExecutePatternDbSQLRecord.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(ExecutePatternDbSQLRecord.RESULTSET_INDEX, "0");
        lastFlowFile.assertAttributeEquals(testAttrName, testAttrValue);
        lastFlowFile.assertAttributeEquals(AbstractExecutePatternDbSQL.INPUT_FLOWFILE_UUID, inputFlowFile.getAttribute(CoreAttributes.UUID.key()));
    }

    @Test
    public void testMaxRowsPerFlowFile() throws Exception {
        // remove previous test database, if any
        runner.setIncomingConnection(false);
        runner.setProperty(AbstractExecutePatternDbSQL.MAX_ROWS_PER_FLOW_FILE, "5");
        runner.setProperty(AbstractExecutePatternDbSQL.OUTPUT_BATCH_SIZE, "0");
        runner.setProperty(AbstractExecutePatternDbSQL.SQL_SELECT_QUERY, "SELECT * FROM TEST_NULL_INT");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecutePatternDbSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExecutePatternDbSQL.REL_SUCCESS, 200);
        runner.assertTransferCount(AbstractExecutePatternDbSQL.REL_FAILURE, 0);
        runner.assertAllFlowFilesContainAttribute(AbstractExecutePatternDbSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_INDEX.key());
        runner.assertAllFlowFilesContainAttribute(AbstractExecutePatternDbSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_ID.key());
        runner.assertAllFlowFilesContainAttribute(AbstractExecutePatternDbSQL.REL_SUCCESS, FragmentAttributes.FRAGMENT_COUNT.key());

        MockFlowFile firstFlowFile = runner.getFlowFilesForRelationship(AbstractExecutePatternDbSQL.REL_SUCCESS).get(0);

        firstFlowFile.assertAttributeEquals(AbstractExecutePatternDbSQL.RESULT_ROW_COUNT, "5");
        firstFlowFile.assertAttributeEquals("record.count", "5");
        firstFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain"); // MockRecordWriter has text/plain MIME type
        firstFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "0");
        firstFlowFile.assertAttributeEquals(AbstractExecutePatternDbSQL.RESULTSET_INDEX, "0");

        MockFlowFile lastFlowFile = runner.getFlowFilesForRelationship(AbstractExecutePatternDbSQL.REL_SUCCESS).get(199);

        lastFlowFile.assertAttributeEquals(AbstractExecutePatternDbSQL.RESULT_ROW_COUNT, "5");
        lastFlowFile.assertAttributeEquals("record.count", "5");
        lastFlowFile.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "text/plain"); // MockRecordWriter has text/plain MIME type
        lastFlowFile.assertAttributeEquals(FragmentAttributes.FRAGMENT_INDEX.key(), "199");
        lastFlowFile.assertAttributeEquals(AbstractExecutePatternDbSQL.RESULTSET_INDEX, "0");
    }

    @Test
    public void testWriteLOBsToAvro() throws Exception {
        //final DBCPService dbcp = new DBCPServiceSimpleImpl("h2");
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ExecutePatternDbSQLRecord.class);
        // remove previous test database, if any
        runner.setValidateExpressionUsage(false);

        runner.setIncomingConnection(false);
        runner.setProperty(AbstractExecutePatternDbSQL.HOSTS,"192.168.133.236:5432");
        runner.setProperty(AbstractExecutePatternDbSQL.DB_DRIVERNAME,"org.postgresql.Driver");
        runner.setProperty(AbstractExecutePatternDbSQL.DB_DRIVER_LOCATION,"E:\\平台文档\\新数据平台文档\\数据接入\\nifi资料\\nifi jar包\\nifi\\postgresql-42.2.8.jar");
        runner.setProperty(AbstractExecutePatternDbSQL.DB_USER,"postgres");
        runner.setProperty(AbstractExecutePatternDbSQL.DB_PASSWORD,"Postgres@dev123");
        runner.setProperty(AbstractExecutePatternDbSQL.DB_URL_PARAMS,"Unicode=true&characterEncoding=UTF-8&tinyInt1isBit=false");
        runner.setProperty(AbstractExecutePatternDbSQL.DATABASE_NAME,"sys_domain");
       // runner.setProperty(AbstractExecutePatternDbSQL.MAX_WAIT_TIME,"500 millis");

        runner.setProperty(AbstractExecutePatternDbSQL.SQL_SELECT_QUERY, "select * from cs_domain");
        AvroRecordSetWriter recordWriter = new AvroRecordSetWriter();
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(recordWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(ExecutePatternDbSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractExecutePatternDbSQL.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AbstractExecutePatternDbSQL.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals(AbstractExecutePatternDbSQL.RESULT_ROW_COUNT, "1");

        ByteArrayInputStream bais = new ByteArrayInputStream(flowFile.toByteArray());
        final DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(bais, new GenericDatumReader<>());
        final Schema avroSchema = dataFileStream.getSchema();
        GenericData.setStringType(avroSchema, GenericData.StringType.String);
        final GenericRecord avroRecord = dataFileStream.next();

        Object imageObj = avroRecord.get("IMAGE");
        assertNotNull(imageObj);
        assertTrue(imageObj instanceof ByteBuffer);
        assertArrayEquals(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF}, ((ByteBuffer) imageObj).array());

        Object wordsObj = avroRecord.get("WORDS");
        assertNotNull(wordsObj);
        assertTrue(wordsObj instanceof Utf8);
        assertEquals("Hello World", wordsObj.toString());

        Object natwordsObj = avroRecord.get("NATWORDS");
        assertNotNull(natwordsObj);
        assertTrue(natwordsObj instanceof Utf8);
        assertEquals("I am an NCLOB", natwordsObj.toString());
    }


    @Test
    public void testWithSqlException() throws Exception {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NO_ROWS");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NO_ROWS (id integer)");

        runner.setIncomingConnection(false);
        // Try a valid SQL statement that will generate an error (val1 does not exist, e.g.)
        runner.setProperty(AbstractExecutePatternDbSQL.SQL_SELECT_QUERY, "SELECT val1 FROM TEST_NO_ROWS");
        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecutePatternDbSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);
        runner.run();

        //No incoming flow file containing a query, and an exception causes no outbound flowfile.
        // There should be no flow files on either relationship
        runner.assertAllFlowFilesTransferred(AbstractExecutePatternDbSQL.REL_FAILURE, 0);
        runner.assertAllFlowFilesTransferred(AbstractExecutePatternDbSQL.REL_SUCCESS, 0);
    }

    public void invokeOnTriggerRecords(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final Map<String, String> attrs, final boolean setQueryProperty)
            throws InitializationException, ClassNotFoundException, SQLException, IOException {

        if (queryTimeout != null) {
            runner.setProperty(AbstractExecutePatternDbSQL.QUERY_TIMEOUT, queryTimeout.toString() + " secs");
        }

        LOGGER.info("test data loaded");

        // ResultSet size will be 1x200x100 = 20 000 rows
        // because of where PER.ID = ${person.id}
        final int nrOfRows = 20000;

        MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1);
        runner.addControllerService("writer", recordWriter);
        runner.setProperty(ExecutePatternDbSQLRecord.RECORD_WRITER_FACTORY, "writer");
        runner.enableControllerService(recordWriter);

        if (incomingFlowFile) {
            // incoming FlowFile content is not used, but attributes are used
            final Map<String, String> attributes = (attrs == null) ? new HashMap<>() : attrs;
            attributes.put("person.id", "10");
            if (!setQueryProperty) {
                runner.enqueue(query.getBytes(), attributes);
            } else {
                runner.enqueue("Hello".getBytes(), attributes);
            }
        }

        if (setQueryProperty) {
            runner.setProperty(AbstractExecutePatternDbSQL.SQL_SELECT_QUERY, query);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(AbstractExecutePatternDbSQL.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(AbstractExecutePatternDbSQL.REL_SUCCESS, AbstractExecutePatternDbSQL.RESULT_QUERY_DURATION);
        runner.assertAllFlowFilesContainAttribute(AbstractExecutePatternDbSQL.REL_SUCCESS, AbstractExecutePatternDbSQL.RESULT_QUERY_EXECUTION_TIME);
        runner.assertAllFlowFilesContainAttribute(AbstractExecutePatternDbSQL.REL_SUCCESS, AbstractExecutePatternDbSQL.RESULT_QUERY_FETCH_TIME);
        runner.assertAllFlowFilesContainAttribute(AbstractExecutePatternDbSQL.REL_SUCCESS, AbstractExecutePatternDbSQL.RESULT_ROW_COUNT);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(AbstractExecutePatternDbSQL.REL_SUCCESS);
        final long executionTime = Long.parseLong(flowfiles.get(0).getAttribute(AbstractExecutePatternDbSQL.RESULT_QUERY_EXECUTION_TIME));
        final long fetchTime = Long.parseLong(flowfiles.get(0).getAttribute(AbstractExecutePatternDbSQL.RESULT_QUERY_FETCH_TIME));
        final long durationTime = Long.parseLong(flowfiles.get(0).getAttribute(AbstractExecutePatternDbSQL.RESULT_QUERY_DURATION));
        assertEquals(durationTime, fetchTime + executionTime);
    }

}
