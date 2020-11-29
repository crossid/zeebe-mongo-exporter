package io.crossid.zeebe.exporter;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import io.crossid.zeebe.exporter.dto.BulkResponse;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.value.DeploymentRecordValue;
import io.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.protocol.record.value.VariableRecordValue;
import io.zeebe.protocol.record.value.WorkflowInstanceRecordValue;
import io.zeebe.protocol.record.value.deployment.DeployedWorkflow;
import org.bson.Document;
import org.slf4j.Logger;
import io.zeebe.protocol.record.ValueType;

import java.util.*;
import java.util.stream.Collectors;


import com.mongodb.client.MongoClients;

class Tuple<X, Y> {
    public final X x;
    public final Y y;
    public Tuple(X x, Y y) {
        this.x = x;
        this.y = y;
    }
}

public class ZeebeMongoClient {
    private final Logger log;
//    private final DateTimeFormatter formatter;
    private final List<Tuple<String,UpdateOneModel<Document>>> bulkOperations;
    private final MongoExporterConfiguration configuration;
    private final MongoClient client;
    public static final String COL_DELIMITER = "_";

    public ZeebeMongoClient(
            final MongoExporterConfiguration configuration, final Logger log) {
        this(configuration, log, new ArrayList<>());
    }

    ZeebeMongoClient(
            final MongoExporterConfiguration configuration,
            final Logger log,
            final List<Tuple<String,UpdateOneModel<Document>>> bulkOperations) {
        this.configuration = configuration;
        this.log = log;
        this.client = createClient();
        this.bulkOperations = bulkOperations;
//        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);
    }

    public MongoClient createClient() {
        return MongoClients.create(configuration.url);
    }

    public void close() {
        client.close();
    }

    public void insert(Record<?> record) {
        bulk(newUpsertCommand(record));
    }

    public void bulk(final List<Tuple<String, UpdateOneModel<Document>>> bulkOperation) {
        // TODO: generalise, cache, etc

        if (bulkOperation == null) {
            return;
        }

        bulkOperations.addAll(bulkOperation);
    }

    /**
     * @throws MongoExporterException if not all items of the bulk were flushed successfully
     */
    public void flush() {
       if (bulkOperations.isEmpty()) {
            return;
        }

//        final int bulkSize = bulkRequest.size();
//        metrics.recordBulkSize(bulkSize);
//        final var bulkMemorySize = getBulkMemorySize();
//        metrics.recordBulkMemorySize(bulkMemorySize);

        try {
            exportBulk();
        } catch (final MongoExporterException e) {
            throw new MongoExporterException("Failed to flush bulk", e);
        }
    }
    /**
     * @throws MongoExporterException if not all items of the bulk were flushed successfully
     */
    private void exportBulk() {
        MongoDatabase db = client.getDatabase(configuration.dbName);
//        var bulkResponse = new BulkResponse();

        // Split the operations to collections
        var ops = new HashMap<String, List<UpdateOneModel<Document>>>();
        for (var op : bulkOperations) {
            if (!ops.containsKey(op.x)) {
                ops.put(op.x, new ArrayList<>());
                ops.get(op.x).add(op.y);
            }
            else {
                var opList = ops.get(op.x);
                if (!opList.get(opList.size() - 1).equals(op.y)) {
                    opList.add(op.y);
                }
            }
        }

        var success = true;
        Exception ex = null;
        String exCollectionName = null;

        for (var collectionName : ops.keySet()) {
            final var operationsPerCollection = ops.get(collectionName);
            if (!operationsPerCollection.isEmpty()) {
                try {
                    var collection = db.getCollection(collectionName);
                    var bulkWriteResult = collection.bulkWrite(operationsPerCollection);
                    operationsPerCollection.clear();
                    log.debug("Flushed to collection {}, {} inserted, {} updated", collectionName, bulkWriteResult.getInsertedCount(), bulkWriteResult.getModifiedCount());
                }
                catch (Exception e) {
                    log.warn(
                            "Failed to flush {} item(s) of bulk request [collection: {}, reason: {}]",
                            operationsPerCollection.size(),
                            collectionName,
                            e.getMessage());
                    success = false;
                    ex = e;
                    exCollectionName = collectionName;
//                    throw new MongoExporterException("Failed to bulk write to collection: " + collectionName, e);
                }
            }
        }

        for (var collectionName : ops.keySet()) {
            final var operationsPerCollection = ops.get(collectionName);
            if (!operationsPerCollection.isEmpty()) {
                List<Tuple<String, UpdateOneModel<Document>>> converted = operationsPerCollection.stream()
                        .map(y -> new Tuple<>(collectionName, y))
                        .collect(Collectors.toCollection(LinkedList::new));

                bulkOperations.addAll(converted);
            }
        }

        if (!success &&  exCollectionName != null) {
            throw new MongoExporterException("Failed to bulk write to collection: " + exCollectionName, ex);
        }
//        return new BulkResponse();
    }


    public boolean shouldFlush() {
        return bulkOperations.size() >= configuration.bulk.size
                || getBulkMemorySize() >= configuration.bulk.memoryLimit;
    }

    // TODO : this never triggers the flush
    private int getBulkMemorySize() {
        return 0;
    }

    private String getCollectionName(final Record<?> record) {
        return indexPrefixForValueType(record.getValueType());
    }

    private String indexPrefixForValueType(final ValueType valueType) {
        return configuration.col.prefix + COL_DELIMITER + valueTypeToString(valueType);
    }

    private static String valueTypeToString(final ValueType valueType) {
        return valueType.name().toLowerCase();
    }

    private List<Tuple<String, UpdateOneModel<Document>>> newUpsertCommand(final Record<?> record) {
        final var valueType = record.getValueType();


        switch (valueType) {
            case JOB: return jobUpsertCommand(record);
            case DEPLOYMENT: return deploymentUpsertCommand(record);
            case WORKFLOW_INSTANCE: return jobUpsertCommand(record);
            case INCIDENT: return jobUpsertCommand(record);
            case MESSAGE: return jobUpsertCommand(record);
            case MESSAGE_SUBSCRIPTION: return jobUpsertCommand(record);
            case WORKFLOW_INSTANCE_SUBSCRIPTION: return jobUpsertCommand(record);
            case JOB_BATCH: return jobUpsertCommand(record);
            case TIMER: return jobUpsertCommand(record);
            case MESSAGE_START_EVENT_SUBSCRIPTION: return jobUpsertCommand(record);
            case VARIABLE: return jobUpsertCommand(record);
            case VARIABLE_DOCUMENT: return jobUpsertCommand(record);
            case WORKFLOW_INSTANCE_CREATION: return jobUpsertCommand(record);
            case ERROR: return jobUpsertCommand(record);
            case WORKFLOW_INSTANCE_RESULT: return jobUpsertCommand(record);
            default: return null;
        }
    }

    private List<Tuple<String, UpdateOneModel<Document>>> jobUpsertCommand(final Record<?> record) {
        var castRecord = (JobRecordValue) record.getValue();

        var document =  new Document("_id", record.getKey())
                .append("jobType", castRecord.getType())
                .append("workflowInstanceKey", castRecord.getWorkflowInstanceKey())
                .append("elementInstanceKey", castRecord.getElementInstanceKey())
                .append("worker", castRecord.getWorker())
                .append("retries", castRecord.getRetries())
                .append("timestamp", new Date(record.getTimestamp()));


        switch (record.getIntent().name()) {
            case "ACTIVATED": document.append("state", "ACTIVATED"); break;
            case "FAILED": document.append("state", "FAILED"); break;
            case "COMPLETED": document.append("state", "COMPLETED"); break;
            case "CANCELED": document.append("state", "CANCELED"); break;
            case "ERROR_THROWN": document.append("state", "ERROR_THROWN"); break;
            case "CREATED":
            case "TIMED_OUT":
            case "RETRIES_UPDATED":
            default:
                document.append("state", "ACTIVATABLE"); break;
        }

        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        result.add(new Tuple<>(getCollectionName(record), new UpdateOneModel<>(
                new Document("_id", record.getKey()),
                document,
                new UpdateOptions().upsert(true)
        )));

        return result;
    }

    private List<Tuple<String, UpdateOneModel<Document>>> deploymentUpsertCommand(final Record<?> record) {
        if (!record.getIntent().name().equals("DISTRIBUTED")) {
            return null;
        }

        var castRecord = (DeploymentRecordValue) record.getValue();

        var result = new ArrayList<Tuple<String, UpdateOneModel<Document>>>();
        var timestamp = new Date(record.getTimestamp());

        for (var workflow : castRecord.getDeployedWorkflows()) {
            result.add(new Tuple<>("", workflowUpsertCommand(workflow, timestamp)));
        }

        return result;
    }

    private UpdateOneModel<Document> workflowUpsertCommand(DeployedWorkflow record, Date timestamp) {
        var document = new Document("_id", record.getWorkflowKey())
                .append("bpmnProcessId", record.getBpmnProcessId())
                .append("version", record.getVersion())
                .append("resource", record.getResourceName())
                .append("timestamp", timestamp);


        return new UpdateOneModel<>(
                new Document("_id", record.getWorkflowKey()),
                document,
                new UpdateOptions().upsert(true)
        );
    }



//
//    private UpdateOneModel<Document> workflowInstanceUpsertCommand(final Record<?> record) {
//        var castRecord = (WorkflowInstanceRecordValue) record.getValue();
//        var document = new Document("_id", castRecord.getWorkflowInstanceKey())
//                .append("bpmnProcessId", castRecord.getBpmnProcessId())
//                .append("version", castRecord.getVersion())
//                .append("workflowKey", castRecord.getWorkflowKey());
//
//        if (castRecord.getParentWorkflowInstanceKey() > 0) {
//            document.append("parentWorkflowInstanceKey", castRecord.getParentWorkflowInstanceKey());
//        }
//
//        if (castRecord.getParentElementInstanceKey() > 0) {
//            document.append("parentElementInstanceKey", castRecord.getParentElementInstanceKey());
//        }
//
//        return new UpdateOneModel<>(
//                new Document("_id", castRecord.getWorkflowInstanceKey()),
//                document,
//                new UpdateOptions().upsert(true)
//        );
//    }
//



}
