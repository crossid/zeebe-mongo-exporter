package io.crossid.zeebe.exporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import io.zeebe.engine.state.instance.Incident;
import io.zeebe.exporter.api.context.Configuration;
import io.zeebe.exporter.api.context.Context;
import io.zeebe.exporter.api.context.Controller;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.intent.Intent;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.protocol.record.value.*;

import java.time.Duration;
import java.util.*;
import java.util.stream.IntStream;

import io.zeebe.test.util.socket.SocketUtil;
import io.zeebe.util.ZbLogger;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoClientTest extends AbstractMongoExporterIntegrationTestCase {
    private static final long RECORD_KEY = 1234L;
    private MongoExporterConfiguration configuration;
    private Logger logSpy;
    private ZeebeMongoClient client;
    private List<Tuple<String, UpdateOneModel<Document>>>  bulkRequest;
    private long lastExportedRecordPosition;

    @Before
    public void init() {
        mongo.withPort(SocketUtil.getNextAddress().getPort()).start();
        configuration = getDefaultConfiguration();
        logSpy = spy(LoggerFactory.getLogger(MongoClientTest.class));
        bulkRequest = new ArrayList<>();
        client = new ZeebeMongoClient(configuration, logSpy, bulkRequest);
    }

    @Test
    public void varsShouldBeExported() {
        final Record<VariableRecordValue> recordMock = mock(Record.class);
        when(recordMock.getPartitionId()).thenReturn(1);
        when(recordMock.getKey()).thenReturn(RECORD_KEY);
        when(recordMock.getValueType()).thenReturn(ValueType.VARIABLE);
    }

    @Test
    public void flowShouldChangeStatus() {
        final Record<WorkflowInstanceRecordValue> recordMock = mock(Record.class);
        when(recordMock.getPartitionId()).thenReturn(1);
        when(recordMock.getKey()).thenReturn(RECORD_KEY);
        when(recordMock.getValueType()).thenReturn(ValueType.WORKFLOW_INSTANCE);
        when(recordMock.toJson()).thenReturn("{}");
        when(recordMock.getIntent()).thenReturn(WorkflowInstanceIntent.ELEMENT_ACTIVATED);

        final WorkflowInstanceRecordValue value = mock(WorkflowInstanceRecordValue.class);
        when(value.getBpmnProcessId()).thenReturn("1");
        when(value.getVersion()).thenReturn(1);
        when(value.getWorkflowKey()).thenReturn(1L);
        when(value.getBpmnElementType()).thenReturn(BpmnElementType.START_EVENT);

        when(recordMock.getValue()).thenReturn(value);

        client.insert(recordMock);
        client.flush();

        when(recordMock.getIntent()).thenReturn(WorkflowInstanceIntent.ELEMENT_COMPLETED);
        when(value.getBpmnElementType()).thenReturn(BpmnElementType.END_EVENT);
        client.insert(recordMock);
        client.flush();


    }

    @Test
    public void incidentShouldBeExported() {
        final Record<IncidentRecordValue> recordMock = mock(Record.class);
        when(recordMock.getPartitionId()).thenReturn(1);
        when(recordMock.getKey()).thenReturn(RECORD_KEY);
        when(recordMock.getValueType()).thenReturn(ValueType.INCIDENT);
        when(recordMock.toJson()).thenReturn("{}");
        Date timestamp = new Date();
        when(recordMock.getTimestamp()).thenReturn(timestamp.getTime());
        when(recordMock.getIntent()).thenReturn(IncidentIntent.CREATED);


        final IncidentRecordValue value = mock(IncidentRecordValue.class);
        when(value.getErrorType()).thenReturn(ErrorType.CONDITION_ERROR);
        when(value.getErrorMessage()).thenReturn("msg");
        when(value.getWorkflowInstanceKey()).thenReturn(Long.valueOf(1));
        when(value.getElementInstanceKey()).thenReturn(Long.valueOf(1));
        when(value.getJobKey()).thenReturn(Long.valueOf(1));
        when(recordMock.getValue()).thenReturn(value);

        client.insert(recordMock);
        client.flush();
    }
}
