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
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.FragmentAttributes;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TestClearTableData {

    private static final Logger LOGGER;

    static {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.ClearTableData", "debug");
        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestClearTableData", "debug");
        LOGGER = LoggerFactory.getLogger(TestClearTableData.class);
    }

    final static String DB_LOCATION = "target/db";

    @BeforeClass
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl("derby");
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(ClearTableData.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(ClearTableData.DBCP_SERVICE, "dbcp");
        runner.setValidateExpressionUsage(false);
    }

    @Test(expected = AssertionError.class)
    public void testNoIncomingConnectionAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(false);
        runner.run();
    }

    @Test
    public void testWithOutputBatching() throws InitializationException, SQLException {
        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
        Statement stmt = con.createStatement();

        try {
            stmt.execute("drop table TEST_NULL_INT");
        } catch (final SQLException sqle) {
        }

        stmt.execute("create table TEST_NULL_INT (id integer not null, val1 integer, val2 integer, constraint my_pk primary key (id))");

        for (int i = 0; i < 1000; i++) {
            stmt.execute("insert into TEST_NULL_INT (id, val1, val2) VALUES (" + i + ", 1, 1)");
        }

        runner.setIncomingConnection(true);
        runner.setProperty(ClearTableData.SQL_DELETE, "delete from ${tableName} where id=${pro_id}");
        runner.setProperty(ClearTableData.TABLE_NAME, "TEST_NULL_INT");
        runner.setProperty(ClearTableData.IS_TRUNCATE_TABLE, "true");
        MockFlowFile flowFile1 = new MockFlowFile(1);
        Map<String,String> attributes = new HashMap<>();
        attributes.put("pro_id","1");
        flowFile1.putAttributes(attributes);
        runner.enqueue(flowFile1);
        runner.run();
        MockFlowFile flowFile2 = new MockFlowFile(2);
        runner.enqueue(flowFile2);
        runner.run();
        stmt.execute("select count(*) from TEST_NULL_INT");
        final ResultSet resultSet = stmt.getResultSet();
        if(resultSet.next()) {
            int count = resultSet.getInt(1);
            assertEquals(1000,count);
        }
    }

}
