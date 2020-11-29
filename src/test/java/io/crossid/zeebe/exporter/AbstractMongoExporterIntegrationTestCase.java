package io.crossid.zeebe.exporter;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.internal.MongoClientImpl;
import io.crossid.zeebe.exporter.util.MongoContainer;
import io.crossid.zeebe.exporter.util.MongoNode;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.test.exporter.ExporterIntegrationRule;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import io.zeebe.util.ZbLogger;

import java.io.IOException;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;

public class AbstractMongoExporterIntegrationTestCase {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public final RecordingExporterTestWatcher recordingExporterTestWatcher =
            new RecordingExporterTestWatcher();

    protected final ExporterIntegrationRule exporterBrokerRule = new ExporterIntegrationRule();

    protected MongoNode<MongoContainer> mongo;
    protected MongoExporterConfiguration configuration;
    protected MongoExporterFaultToleranceIT.MongoTestClient mgoClient;

    @Before
    public void setUp() {
        mongo = new MongoContainer();
    }

    @After
    public void tearDown() throws IOException {
        if (mgoClient != null) {
            mgoClient.close();
            mgoClient = null;
        }

        exporterBrokerRule.stop();
        mongo.stop();
        configuration = null;
    }

    protected void assertColsSettings() {
    }

    protected void assertRecordExported(final Record<?> record) {
        final Map<String, Object> source = mgoClient.getDocument(record);
        assertThat(source)
                .withFailMessage("Failed to fetch record %s from mongo", record)
                .isNotNull()
                .isEqualTo(recordToMap(record));
    }

    protected MongoTestClient createMongoClient(
            final MongoExporterConfiguration configuration) {
        return new MongoTestClient(
                configuration, new ZbLogger("io.crossid.zeebe.exporter.mongo"));
    }

    protected Map<String, Object> recordToMap(final Record<?> record) {
        final JsonNode jsonNode;
        try {
            jsonNode = MAPPER.readTree(record.toJson());
        } catch (final IOException e) {
            throw new AssertionError("Failed to deserialize json of record " + record.toJson(), e);
        }

        return MAPPER.convertValue(jsonNode, new TypeReference<Map<String, Object>>() {
        });
    }

//    private int numberOfShardsForIndex(final String indexName) {
//        if (indexName.startsWith(
//                mgoClient.indexPrefixForValueTypeWithDelimiter(ValueType.WORKFLOW_INSTANCE))
//                || indexName.startsWith(mgoClient.indexPrefixForValueTypeWithDelimiter(ValueType.JOB))) {
//            return 3;
//        } else {
//            return 1;
//        }
//    }

    protected MongoExporterConfiguration getDefaultConfiguration() {
        final MongoExporterConfiguration configuration =
                new MongoExporterConfiguration();

        configuration.url = mongo.getUrl();
        configuration.bulk.delay = 1;
        configuration.bulk.size = 1;

        configuration.col.prefix = "test-record";
        configuration.col.createCollections = true;
        configuration.col.command = true;
        configuration.col.event = true;
        configuration.col.rejection = true;
        configuration.col.deployment = true;
        configuration.col.error = true;
        configuration.col.incident = true;
        configuration.col.job = true;
        configuration.col.jobBatch = true;
        configuration.col.message = true;
        configuration.col.messageSubscription = true;
        configuration.col.variable = true;
        configuration.col.variableDocument = true;
        configuration.col.workflowInstance = true;
        configuration.col.workflowInstanceCreation = true;
        configuration.col.workflowInstanceSubscription = true;

        return configuration;
    }

    protected static class MongoTestClient extends MgoClient {
        private static final ObjectMapper MAPPER = new ObjectMapper();

        MongoTestClient(
                final MongoExporterConfiguration configuration, final Logger log) {
            super(configuration, log);
        }

        Map<String, Object> getDocument(final Record<?> record) {
//            final var request =
//                    new Request("GET", "/" + indexFor(record) + "/" + typeFor(record) + "/" + idFor(record));
//            request.addParameter("routing", String.valueOf(record.getPartitionId()));
//            try {
//                final var response = client.performRequest(request);
//                final var document =
//                        MAPPER.readValue(response.getEntity().getContent(), GetDocumentResponse.class);
//                return document.getSource();
//            } catch (final IOException e) {
//                throw new ElasticsearchExporterException(
//                        "Failed to get record " + idFor(record) + " from index " + indexFor(record));
//            }
//        }

            return null;
        }
    }
}
