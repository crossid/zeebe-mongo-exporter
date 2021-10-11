package io.crossid.zeebe.exporter;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.test.util.TestUtil;
import io.camunda.zeebe.test.util.record.RecordingExporter;
import io.camunda.zeebe.test.util.socket.SocketUtil;
import org.junit.Test;

public class MongoExporterFaultToleranceIT extends AbstractMongoExporterIntegrationTestCase {
    /* TODO
    @Test
    public void shouldExportEvenIfMongoNotInitiallyReachable() {
        // given
        mongo.withPort(SocketUtil.getNextAddress().getPort());
        configuration = getDefaultConfiguration();
        configuration.col.prefix = "zeebe";
        mgoClient = createMongoClient(configuration);

        // when
        exporterBrokerRule.configure("mongo", MongoExporter.class, configuration);
        exporterBrokerRule.start();
        exporterBrokerRule.publishMessage("message", "123");
        mongo.start();

        // then
        RecordingExporter.messageRecords()
                .withCorrelationKey("123")
                .withName("message")
                .forEach(r -> TestUtil.waitUntil(() -> wasExported(r)));
        assertColsSettings();
    }*/

    private boolean wasExported(final Record<?> record) {
        try {
            return mgoClient.getDocument(record) != null;
        } catch (final Exception e) {
            // suppress exception in order to retry and see if it was exported yet or not
            // the exception can occur since elastic may not be ready yet, or maybe the index hasn't been
            // created yet, etc.
        }

        return false;
    }
}
