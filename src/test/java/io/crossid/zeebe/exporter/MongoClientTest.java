package io.crossid.zeebe.exporter;

import com.mongodb.client.model.UpdateOneModel;
import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.IncidentIntent;
import io.camunda.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.mockito.Mockito.*;

public class MongoClientTest extends AbstractMongoExporterIntegrationTestCase {
    private static final long RECORD_KEY = 1234L;
    private MongoExporterConfiguration configuration;
    private Logger logSpy;
    private ZeebeMongoClient client;
    private List<Tuple<String, UpdateOneModel<Document>>>  bulkRequest;
    private long lastExportedRecordPosition;

//    @Before
    public void init() {
//        mongo.withPort(SocketUtil.getNextAddress().getPort()).start();
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
        final Record<ProcessInstanceRecordValue> recordMock = mock(Record.class);
        when(recordMock.getPartitionId()).thenReturn(1);
        when(recordMock.getKey()).thenReturn(RECORD_KEY);
        when(recordMock.getValueType()).thenReturn(ValueType.PROCESS_INSTANCE);
        when(recordMock.toJson()).thenReturn("{}");
        when(recordMock.getIntent()).thenReturn(ProcessInstanceIntent.ELEMENT_ACTIVATED);

        final ProcessInstanceRecordValue value = mock(ProcessInstanceRecordValue.class);
        when(value.getBpmnProcessId()).thenReturn("1");
        when(value.getVersion()).thenReturn(1);
        when(value.getProcessDefinitionKey()).thenReturn(1L);
        when(value.getBpmnElementType()).thenReturn(BpmnElementType.START_EVENT);

        when(recordMock.getValue()).thenReturn(value);

        client.insert(recordMock);
        client.flush();

        when(recordMock.getIntent()).thenReturn(ProcessInstanceIntent.ELEMENT_COMPLETED);
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
        when(value.getProcessInstanceKey()).thenReturn(Long.valueOf(1));
        when(value.getElementInstanceKey()).thenReturn(Long.valueOf(1));
        when(value.getJobKey()).thenReturn(Long.valueOf(1));
        when(recordMock.getValue()).thenReturn(value);

        client.insert(recordMock);
        client.flush();
    }

    @Test
    public void timersShouldBeExported() {
        final Record<TimerRecordValue> recordMock = mock(Record.class);
        when(recordMock.getPartitionId()).thenReturn(1);
        when(recordMock.getKey()).thenReturn(RECORD_KEY);
        when(recordMock.getValueType()).thenReturn(ValueType.TIMER);

        var timeStamp = new Date().getTime();
        when(recordMock.getTimestamp()).thenReturn(timeStamp);
        when(recordMock.getIntent()).thenReturn(IncidentIntent.CREATED);


        final TimerRecordValue value = mock(TimerRecordValue.class);
        var dueDate = timeStamp + 1000000;
        when(value.getDueDate()).thenReturn(dueDate);
        when(value.getRepetitions()).thenReturn(1);
        when(value.getProcessDefinitionKey()).thenReturn(1L);
        when(value.getProcessInstanceKey()).thenReturn(1L);
        when(value.getElementInstanceKey()).thenReturn(1L);

        when(recordMock.getValue()).thenReturn(value);

        client.insert(recordMock);
        client.flush();
    }

    @Test
    public void messageSubscriptionShouldBeExported() {
        final Record<MessageSubscriptionRecordValue> recordMock = mock(Record.class);
        when(recordMock.getPartitionId()).thenReturn(1);
        when(recordMock.getKey()).thenReturn(RECORD_KEY);
        when(recordMock.getValueType()).thenReturn(ValueType.MESSAGE_SUBSCRIPTION);
        when(recordMock.getIntent()).thenReturn(MessageSubscriptionIntent.CORRELATED);

        final MessageSubscriptionRecordValue value = mock(MessageSubscriptionRecordValue.class);
        when(value.getProcessInstanceKey()).thenReturn(1L);
        when(value.getElementInstanceKey()).thenReturn(1L);

        when(recordMock.getValue()).thenReturn(value);

        client.insert(recordMock);
        client.flush();
    }

}
