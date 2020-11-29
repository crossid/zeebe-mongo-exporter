package io.crossid.zeebe.exporter;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.crossid.zeebe.exporter.dto.BulkItemError;
import io.crossid.zeebe.exporter.dto.BulkResponse;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.intent.Intent;
import io.zeebe.protocol.record.value.DeploymentRecordValue;
import io.zeebe.protocol.record.value.VariableRecordValue;
import io.zeebe.protocol.record.value.deployment.DeployedWorkflow;
import io.zeebe.protocol.record.value.deployment.DeploymentResource;
import org.bson.Document;
import org.slf4j.Logger;
import io.zeebe.protocol.record.ValueType;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import com.mongodb.client.MongoClients;

import static io.zeebe.protocol.record.intent.DeploymentIntent.DISTRIBUTED;


public class MgoClient {
    private final Logger log;
    private final DateTimeFormatter formatter;
    private List<String> bulkRequest;
    private final MongoExporterConfiguration configuration;
    private MongoClient client;
    public static final String COL_DELIMITER = "_";

    public MgoClient(
            final MongoExporterConfiguration configuration, final Logger log) {
        this(configuration, log, new ArrayList<>());
    }

    MgoClient(
            final MongoExporterConfiguration configuration,
            final Logger log,
            final List<String> bulkRequest) {
        this.configuration = configuration;
        this.log = log;
        this.client = createClient();
        this.bulkRequest = bulkRequest;
        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    }

    public MongoClient createClient() {
        return MongoClients.create(configuration.url);
    }

    public void close() throws IOException {
        client.close();
    }

    public void insert(Record record) {
        checkRecord(record);
        bulk(newInsertCommand(record), record);
    }

    private void checkRecord(final Record<?> record) {
        if (record.getValueType() == ValueType.VARIABLE) {
            checkVariableRecordValue((Record<VariableRecordValue>) record);
        }
    }

    private void checkVariableRecordValue(final Record<VariableRecordValue> record) {
        final VariableRecordValue value = record.getValue();
        final int size = value.getValue().getBytes().length;

        if (size > configuration.col.ignoreVariablesAbove) {
            log.warn(
                    "Variable {key: {}, name: {}, variableScope: {}, workflowInstanceKey: {}} exceeded max size of {} bytes with a size of {} bytes. As a consequence this variable is not index by elasticsearch.",
                    record.getKey(),
                    value.getName(),
                    value.getScopeKey(),
                    value.getWorkflowInstanceKey(),
                    configuration.col.ignoreVariablesAbove,
                    size);
        }
    }

    public void bulk(final Map<String, Object> command, final Record<?> record) {
        // TODO: generalise, cache, etc
        MongoDatabase db = client.getDatabase(configuration.dbName);
        String colName = colName(record);
        String id = idFor(record);
        MongoCollection<Document> col = db.getCollection(colName);
        Document doc = Document.parse(record.toJson());
        doc.put("_id", id);
        col.insertOne(doc);


//        final String serializedCommand;
//
//        try {
//            serializedCommand = MAPPER.writeValueAsString(command);
//        } catch (final IOException e) {
//            throw new ElasticsearchExporterException(
//                    "Failed to serialize bulk request command to JSON", e);
//        }
//
//        final String jsonCommand = serializedCommand + "\n" + record.toJson();
//         don't re-append when retrying same record, to avoid OOM
//        if (bulkRequest.isEmpty() || !bulkRequest.get(bulkRequest.size() - 1).equals(jsonCommand)) {
//            bulkRequest.add(jsonCommand);
//        }
    }

    /**
     * @throws MongoExporterException if not all items of the bulk were flushed successfully
     */
    public void flush() {
        if (bulkRequest.isEmpty()) {
            return;
        }

        final int bulkSize = bulkRequest.size();
//        metrics.recordBulkSize(bulkSize);
        final var bulkMemorySize = getBulkMemorySize();
//        metrics.recordBulkMemorySize(bulkMemorySize);

        final BulkResponse bulkResponse;
        try {
            bulkResponse = exportBulk();

        } catch (final IOException e) {
            throw new MongoExporterException("Failed to flush bulk", e);
        }

//        final var success = checkBulkResponse(bulkResponse);
//        if (!success) {
//            throw new ElasticsearchExporterException("Failed to flush all items of the bulk");
//        }

        // all records where flushed, create new bulk request, otherwise retry next time
        bulkRequest = new ArrayList<>();
    }

    private boolean checkBulkResponse(final BulkResponse bulkResponse) {
        final var hasErrors = bulkResponse.hasErrors();
        if (hasErrors) {
            bulkResponse.getItems().stream()
                    .flatMap(item -> Optional.ofNullable(item.getIndex()).stream())
                    .flatMap(index -> Optional.ofNullable(index.getError()).stream())
                    .collect(Collectors.groupingBy(BulkItemError::getType))
                    .forEach(
                            (errorType, errors) ->
                                    log.warn(
                                            "Failed to flush {} item(s) of bulk request [type: {}, reason: {}]",
                                            errors.size(),
                                            errorType,
                                            errors.get(0).getReason()));
        }

        return !hasErrors;
    }

    private BulkResponse exportBulk() throws IOException {
//        try (final Histogram.Timer timer = metrics.measureFlushDuration()) {

//            final var request = new Request("POST", "/_bulk");
//            request.setJsonEntity(String.join("\n", bulkRequest) + "\n");
//
//            final var response = client.performRequest(request);
//
//            return MAPPER.readValue(response.getEntity().getContent(), BulkResponse.class);
//        }

        return new BulkResponse();
    }


    public boolean shouldFlush() {
        return bulkRequest.size() >= configuration.bulk.size
                || getBulkMemorySize() >= configuration.bulk.memoryLimit;
    }

    private int getBulkMemorySize() {
        return bulkRequest.stream().mapToInt(String::length).sum();
    }

    public boolean createCollection(ValueType valueType) {
        return true;
    }

    public String colName(final Record<?> record) {
        return indexPrefixForValueType(record.getValueType());
//                + COL_DELIMITER;
//                + formatter.format(record.getTimestamp());
    }

    private String indexPrefixForValueType(final ValueType valueType) {
        return configuration.col.prefix + "-" + valueTypeToString(valueType);
    }

    private static String valueTypeToString(final ValueType valueType) {
        return valueType.name().toLowerCase().replaceAll("_", "-");
    }

    protected String idFor(final Record<?> record) {
        return record.getPartitionId() + "-" + record.getPosition();
    }

    private Map<String, Object> newInsertCommand(final Record<?> record) {
        final Map<String, Object> command = new HashMap<>();
        final Map<String, Object> contents = new HashMap<>();
//        contents.put("_index", indexFor(record));
//        contents.put("_id", idFor(record));
//        contents.put("routing", String.valueOf(record.getPartitionId()));
//        command.put("index", contents);
        return command;
    }

}
