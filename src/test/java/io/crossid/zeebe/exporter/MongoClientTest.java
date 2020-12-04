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
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.intent.Intent;
import io.zeebe.protocol.record.value.ErrorType;
import io.zeebe.protocol.record.value.IncidentRecordValue;
import io.zeebe.protocol.record.value.VariableRecordValue;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

import io.zeebe.test.util.socket.SocketUtil;
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

    @Before
    public void init() {
        mongo.withPort(SocketUtil.getNextAddress().getPort()).start();
        configuration = getDefaultConfiguration();
        logSpy = spy(LoggerFactory.getLogger(MongoClientTest.class));
        bulkRequest = new ArrayList<>();
        client = new ZeebeMongoClient(configuration, logSpy, bulkRequest);
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

    @Test
    public void shouldNotLogWarningWhenIndexingSmallVariableValue() {
        // given
//        final String variableValue = "x".repeat(configuration.col.ignoreVariablesAbove);

        final Record<VariableRecordValue> recordMock = mock(Record.class);
        when(recordMock.getPartitionId()).thenReturn(1);
        when(recordMock.getKey()).thenReturn(RECORD_KEY);
        when(recordMock.getValueType()).thenReturn(ValueType.VARIABLE);
        when(recordMock.toJson()).thenReturn("{}");

        final VariableRecordValue value = mock(VariableRecordValue.class);
        when(value.getValue()).thenReturn("1234");
        when(recordMock.getValue()).thenReturn(value);

        // when
        client.insert(recordMock);
        client.flush();

        // then
        verify(logSpy, never()).warn(anyString(), ArgumentMatchers.<Object[]>any());
    }

    @Test
    public void shouldLogWarnWhenIndexingLargeVariableValue() {
        // given
        final String variableName = "varName";
        final String variableValue = "x".repeat(configuration.col.ignoreVariablesAbove + 1);
        final long scopeKey = 1234L;
        final long workflowInstanceKey = 5678L;

        final Record<VariableRecordValue> recordMock = mock(Record.class);
        when(recordMock.getPartitionId()).thenReturn(1);
        when(recordMock.getKey()).thenReturn(RECORD_KEY);
        when(recordMock.getValueType()).thenReturn(ValueType.VARIABLE);
        when(recordMock.toJson()).thenReturn("{}");

        final VariableRecordValue value = mock(VariableRecordValue.class);
        when(value.getName()).thenReturn(variableName);
        when(value.getValue()).thenReturn(variableValue);
        when(value.getScopeKey()).thenReturn(scopeKey);
        when(value.getWorkflowInstanceKey()).thenReturn(workflowInstanceKey);

        when(recordMock.getValue()).thenReturn(value);

        // when
        client.insert(recordMock);

        // then
        final ArgumentCaptor<Object[]> argumentCaptor = ArgumentCaptor.forClass(Object[].class);

        verify(logSpy).warn(anyString(), argumentCaptor.capture());

        // required to explicitly cast List<Objects[]> to List<Object>
        final List<Object> args = new ArrayList<>(argumentCaptor.getAllValues());
        assertThat(args)
                .contains(
                        RECORD_KEY,
                        variableName,
                        variableValue.getBytes().length,
                        scopeKey,
                        workflowInstanceKey);
    }

    @Test
    public void shouldThrowExceptionIfFailToFlushBulk() {
        // given
        final int bulkSize = 10;

        final Record<VariableRecordValue> recordMock = mock(Record.class);
        when(recordMock.getPartitionId()).thenReturn(1);
        when(recordMock.getValueType()).thenReturn(ValueType.WORKFLOW_INSTANCE);

        // bulk contains records that fail on flush
        IntStream.range(0, bulkSize)
                .forEach(
                        i -> {
                            when(recordMock.getKey()).thenReturn(RECORD_KEY + i);
                            when(recordMock.toJson()).thenReturn("invalid-json-" + i);
                            client.insert(recordMock);
                        });

        // and one valid record
        when(recordMock.getKey()).thenReturn(RECORD_KEY + bulkSize);
        when(recordMock.toJson()).thenReturn("{}");
        client.insert(recordMock);

        // when/then
        assertThatThrownBy(client::flush)
                .isInstanceOf(MongoExporterException.class)
                .hasMessage("Failed to flush all items of the bulk");

        verify(logSpy)
                .warn(
                        "Failed to flush {} item(s) of bulk request [type: {}, reason: {}]",
                        bulkSize,
                        "mapper_parsing_exception",
                        "failed to parse");
    }
}
