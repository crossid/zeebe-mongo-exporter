package io.crossid.zeebe.exporter;


import io.camunda.zeebe.exporter.api.context.Configuration;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.util.logging.RecordingAppender;
import io.camunda.zeebe.protocol.record.Record;

import java.time.Duration;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class MongoExporterTest_OLD {
    private MongoExporterConfiguration config;
    private ZeebeMongoClient zeebeMongoClient;

    private long lastExportedRecordPosition;

    @Before
    public void setUp() {
        config = new MongoExporterConfiguration();
        zeebeMongoClient = mockMongoClient();
    }

    @Test
    public void shouldCreateCollections() {
        // given
        config.col.prefix = "foo-bar";
        config.col.createCollections = true;
        config.col.deployment = true;
        config.col.incident = true;
        config.col.job = true;
        config.col.jobBatch = true;
        config.col.message = true;
        config.col.messageSubscription = true;
//        config.col.raft = true;
        config.col.workflowInstance = true;
        config.col.workflowInstanceSubscription = true;

        // when
        createExporter(config);

        // then
//        verify(mgoClient).putIndexTemplate("foo-bar", ZEEBE_RECORD_TEMPLATE_JSON);
        verify(zeebeMongoClient).createCollection(ValueType.DEPLOYMENT);
        verify(zeebeMongoClient).createCollection(ValueType.INCIDENT);
        verify(zeebeMongoClient).createCollection(ValueType.JOB);
        verify(zeebeMongoClient).createCollection(ValueType.JOB_BATCH);
        verify(zeebeMongoClient).createCollection(ValueType.MESSAGE);
        verify(zeebeMongoClient).createCollection(ValueType.MESSAGE_SUBSCRIPTION);
//        verify(esClient).putIndexTemplate(ValueType.RAFT);
        verify(zeebeMongoClient).createCollection(ValueType.PROCESS_INSTANCE);
        verify(zeebeMongoClient).createCollection(ValueType.PROCESS_MESSAGE_SUBSCRIPTION);
    }

    @Test
    public void shouldExportEnabledValueTypes() {
        // given
        config.col.event = true;
        config.col.deployment = true;
        config.col.incident = true;
        config.col.job = true;
        config.col.jobBatch = true;
        config.col.message = true;
        config.col.messageSubscription = true;
//        config.col.raft = true;
        config.col.workflowInstance = true;
        config.col.workflowInstanceSubscription = true;

        final MongoExporter exporter = createExporter(config);

        final ValueType[] valueTypes =
                new ValueType[]{
                        ValueType.DEPLOYMENT,
                        ValueType.INCIDENT,
                        ValueType.JOB,
                        ValueType.JOB_BATCH,
                        ValueType.MESSAGE,
                        ValueType.MESSAGE_SUBSCRIPTION,
//                        ValueType.RAFT,
                        ValueType.PROCESS_INSTANCE,
                        ValueType.PROCESS_MESSAGE_SUBSCRIPTION
                };

        // when - then
        for (ValueType valueType : valueTypes) {
            final Record record = mockRecord(valueType, RecordType.EVENT);
            exporter.export(record);
            verify(zeebeMongoClient).insert(record);
        }
    }

    @Test
    public void shouldNotExportDisabledValueTypes() {
        // given
        config.col.event = true;
        config.col.deployment = false;
        config.col.incident = false;
        config.col.job = false;
        config.col.jobBatch = false;
        config.col.message = false;
        config.col.messageSubscription = false;
//        config.col.raft = false;
        config.col.workflowInstance = false;
        config.col.workflowInstanceSubscription = false;

        final MongoExporter exporter = createExporter(config);

        final ValueType[] valueTypes =
                new ValueType[]{
                        ValueType.DEPLOYMENT,
                        ValueType.INCIDENT,
                        ValueType.JOB,
                        ValueType.JOB_BATCH,
                        ValueType.MESSAGE,
                        ValueType.MESSAGE_SUBSCRIPTION,
//                        ValueType.RAFT,
                        ValueType.PROCESS_INSTANCE,
                        ValueType.PROCESS_MESSAGE_SUBSCRIPTION
                };

        // when - then
        for (ValueType valueType : valueTypes) {
            final Record record = mockRecord(valueType, RecordType.EVENT);
            exporter.export(record);
            verify(zeebeMongoClient, never()).insert(record);
        }
    }

    @Test
    public void shouldExportEnabledRecordTypes() {
        // given
        config.col.command = true;
        config.col.event = true;
        config.col.rejection = true;
        config.col.deployment = true;

        final MongoExporter exporter = createExporter(config);

        final RecordType[] recordTypes =
                new RecordType[]{RecordType.COMMAND, RecordType.EVENT, RecordType.COMMAND_REJECTION};

        // when - then
        for (RecordType recordType : recordTypes) {
            final Record record = mockRecord(ValueType.DEPLOYMENT, recordType);
            exporter.export(record);
            verify(zeebeMongoClient).insert(record);
        }
    }

    @Test
    public void shouldNotExportDisabledRecordTypes() {
        // given
        config.col.command = false;
        config.col.event = false;
        config.col.rejection = false;
        config.col.deployment = true;

        final MongoExporter exporter = createExporter(config);

        final RecordType[] recordTypes =
                new RecordType[]{RecordType.COMMAND, RecordType.EVENT, RecordType.COMMAND_REJECTION};

        // when - then
        for (RecordType recordType : recordTypes) {
            final Record record = mockRecord(ValueType.DEPLOYMENT, recordType);
            exporter.export(record);
            verify(zeebeMongoClient, never()).insert(record);
        }
    }

    @Test
    public void shouldIgnoreUnknownValueType() {
        // given
        config.col.event = true;
        final MongoExporter exporter = createExporter(config);
        final Record record = mockRecord(ValueType.SBE_UNKNOWN, RecordType.EVENT);

        // when
        exporter.export(record);

        // then
        verify(zeebeMongoClient, never()).insert(record);
    }

    @Test
    public void shouldUpdateLastPositionOnFlush() {
        // given
        final MongoExporter exporter = createExporter(config);
        when(zeebeMongoClient.shouldFlush()).thenReturn(true);

        final long position = 1234L;
        final Record record = mockRecord(ValueType.PROCESS_INSTANCE, RecordType.EVENT);
        when(record.getPosition()).thenReturn(position);

        // when
        exporter.export(record);

        // then
        assertThat(lastExportedRecordPosition).isEqualTo(position);
    }

    @Test
    public void shouldFlushOnClose() {
        // given
        final MongoExporter exporter = createExporter(config);

        // when
        exporter.close();

        // then
        verify(zeebeMongoClient).flush();
    }

    private MongoExporter createExporter(
            final MongoExporterConfiguration configuration) {
        final MongoExporter exporter =
                new MongoExporter() {
                    @Override
                    protected ZeebeMongoClient createClient() {
                        return zeebeMongoClient;
                    }
                };
        exporter.configure(createContext(configuration));
        exporter.open(createController());
        return exporter;
    }

    private Context createContext(final MongoExporterConfiguration configuration) {
        return new Context() {
            @Override
            public Logger getLogger() {
                return  LoggerFactory.getLogger("io.crossid.zeebe.exporter.mongo");
            }

            @Override
            public Configuration getConfiguration() {
                return new Configuration() {
                    @Override
                    public String getId() {
                        return "elasticsearch";
                    }

                    @Override
                    public Map<String, Object> getArguments() {
                        throw new UnsupportedOperationException("not supported in test case");
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public <T> T instantiate(Class<T> configClass) {
                        return (T) configuration;
                    }
                };
            }

            @Override
            public void setFilter(RecordFilter recordFilter) {

            }
        };
    }

    private Controller createController() {
        return new Controller() {
            @Override
            public void updateLastExportedRecordPosition(long position) {
                lastExportedRecordPosition = position;
            }

            @Override
            public io.camunda.zeebe.exporter.api.context.ScheduledTask scheduleCancellableTask(Duration duration, Runnable runnable) {
                // ignore
                return null;
            }
        };
    }

    private ZeebeMongoClient mockMongoClient() {
        final ZeebeMongoClient client = mock(ZeebeMongoClient.class);
//        when(client.flush()).thenReturn(true);
        // todo needed?
//        when(client.createCollection(any(ValueType.class))).thenReturn(true);
//        when(client.createCollection(anyString())).thenReturn(true);
        return client;
    }


    private Record mockRecord(final ValueType valueType, final RecordType recordType) {
        final Record record = mock(Record.class);
        when(record.getValueType()).thenReturn(valueType);
        when(record.getRecordType()).thenReturn(recordType);

        return record;
    }

}
