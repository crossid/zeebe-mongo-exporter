package io.crossid.zeebe.exporter;

import io.crossid.zeebe.exporter.MongoExporterConfiguration.ColConfiguration;
import io.zeebe.exporter.api.Exporter;
import io.zeebe.exporter.api.ExporterException;
import io.zeebe.exporter.api.context.Context;
import io.zeebe.exporter.api.context.Controller;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.ValueType;

import java.time.Duration;

import org.slf4j.Logger;

// implements https://github.com/zeebe-io/zeebe/issues/1331
public class MongoExporter implements Exporter {
    // by default, the bulk request may not be bigger than 100MB
    private static final int RECOMMENDED_MAX_BULK_MEMORY_LIMIT = 100 * 1024 * 1024;

    private Logger log;
    private Controller controller;

    private MongoExporterConfiguration configuration;
    private ZeebeMongoClient client;

    private long lastPosition = -1;
    private boolean colsCreated;

    @Override
    public void configure(Context context) {
        log = context.getLogger();
        configuration =
                context.getConfiguration().instantiate(MongoExporterConfiguration.class);
        log.debug("Exporter configured with {}", configuration);
        validate(configuration);
        context.setFilter(new MongoRecordFilter(configuration));
    }

    @Override
    public void open(Controller controller) {
        this.controller = controller;
        client = createClient();

        scheduleDelayedFlush();
        log.info("Exporter opened");
    }

    @Override
    public void close() {

        try {
            flush();
        } catch (final Exception e) {
            log.warn("Failed to flush records before closing exporter.", e);
        }

        try {
            client.close();
        } catch (final Exception e) {
            log.warn("Failed to close elasticsearch client", e);
        }

        log.info("Exporter closed");
    }

    @Override
    public void export(final Record record) {
        if (!colsCreated) {
            createCols();
        }

        client.insert(record);
        lastPosition = record.getPosition();

        if (client.shouldFlush()) {
            flush();
        }
    }

    private void validate(final MongoExporterConfiguration configuration) {
        if (configuration.col.prefix != null && configuration.col.prefix.contains("_")) {
            throw new ExporterException(
                    String.format(
                            "Mongo prefix must not contain underscore. Current value: %s",
                            configuration.col.prefix));
        }

        if (configuration.bulk.memoryLimit > RECOMMENDED_MAX_BULK_MEMORY_LIMIT) {
            log.warn(
                    "The bulk memory limit is set to more than {} bytes. It is recommended to set the limit between 5 to 15 MB.",
                    RECOMMENDED_MAX_BULK_MEMORY_LIMIT);
        }
    }

    protected ZeebeMongoClient createClient() {
        return new ZeebeMongoClient(configuration, log);
    }

    private void flushAndReschedule() {
        try {
            flush();
        } catch (final Exception e) {
            log.error(
                    "Unexpected exception occurred on periodically flushing bulk, will retry later.", e);
        }
        scheduleDelayedFlush();
    }

    private void scheduleDelayedFlush() {
        controller.scheduleTask(Duration.ofSeconds(configuration.bulk.delay), this::flushAndReschedule);
    }

    private void flush() {
        client.flush();
        controller.updateLastExportedRecordPosition(lastPosition);
    }

    private void createCols() {
        final ColConfiguration col = configuration.col;

        if (col.createCollections) {
            if (col.deployment) {
                createValueCol(ValueType.DEPLOYMENT);
            }
            if (col.error) {
                createValueCol(ValueType.ERROR);
            }
            if (col.incident) {
                createValueCol(ValueType.INCIDENT);
            }
            if (col.job) {
                createValueCol(ValueType.JOB);
            }
            if (col.jobBatch) {
                createValueCol(ValueType.JOB_BATCH);
            }
            if (col.message) {
                createValueCol(ValueType.MESSAGE);
            }
            if (col.messageSubscription) {
                createValueCol(ValueType.MESSAGE_SUBSCRIPTION);
            }
            if (col.variable) {
                createValueCol(ValueType.VARIABLE);
            }
            if (col.variableDocument) {
                createValueCol(ValueType.VARIABLE_DOCUMENT);
            }
            if (col.workflowInstance) {
                createValueCol(ValueType.WORKFLOW_INSTANCE);
            }
            if (col.workflowInstanceCreation) {
                createValueCol(ValueType.WORKFLOW_INSTANCE_CREATION);
            }
            if (col.workflowInstanceSubscription) {
                createValueCol(ValueType.WORKFLOW_INSTANCE_SUBSCRIPTION);
            }
        }

        colsCreated = true;
    }

    private void createValueCol(final ValueType valueType) {
        if (!client.createCollection(valueType)) {
            log.warn("Put index template for value type {} was not acknowledged", valueType);
        }
    }

    private static class MongoRecordFilter implements Context.RecordFilter {

        private final MongoExporterConfiguration configuration;

        MongoRecordFilter(final MongoExporterConfiguration configuration) {
            this.configuration = configuration;
        }

        @Override
        public boolean acceptType(final RecordType recordType) {
            return configuration.shouldInsertRecordType(recordType);
        }

        @Override
        public boolean acceptValue(final ValueType valueType) {
            return configuration.shouldInsertValueType(valueType);
        }
    }
}
