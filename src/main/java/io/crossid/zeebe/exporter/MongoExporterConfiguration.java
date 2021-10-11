package io.crossid.zeebe.exporter;

import io.camunda.zeebe.protocol.record.Record;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.ValueType;

public class MongoExporterConfiguration {
    // mongo url
    public String url = "http://localhost:27017";
    public String dbName = "zeebe";

    public ColConfiguration col = new ColConfiguration();
    public ColConfiguration.BulkConfiguration bulk = new ColConfiguration.BulkConfiguration();

    @Override
    public String toString() {
        return "MongoExporterConfiguration{"
                + "url='"
                + url
                + '\''
                + ", index="
                + col
                + ", bulk="
                + bulk
                + '}';
    }

    private boolean shouldInsertRecord(Record<?> record) {
        return shouldInsertRecordType(record.getRecordType()) && shouldInsertValueType(record.getValueType());
    }

    public boolean shouldInsertValueType(final ValueType valueType) {
        switch (valueType) {
            case DEPLOYMENT:
                return col.deployment;
            case ERROR:
                return col.error;
            case INCIDENT:
                return col.incident;
            case JOB:
                return col.job;
            case JOB_BATCH:
                return col.jobBatch;
            case MESSAGE:
                return col.message;
            case MESSAGE_SUBSCRIPTION:
                return col.messageSubscription;
            case VARIABLE:
                return col.variable;
            case VARIABLE_DOCUMENT:
                return col.variableDocument;
            case PROCESS_INSTANCE:
                return col.workflowInstance;
            case PROCESS_INSTANCE_CREATION:
                return col.workflowInstanceCreation;
            case PROCESS_MESSAGE_SUBSCRIPTION:
                return col.workflowInstanceSubscription;
            case TIMER:
                return col.timers;
            default:
                return false;
        }
    }

    public boolean shouldInsertRecordType(final RecordType recordType) {
        switch (recordType) {
            case EVENT:
                return col.event;
            case COMMAND:
                return col.command;
            case COMMAND_REJECTION:
                return col.rejection;
            default:
                return false;
        }
    }

    public static class ColConfiguration {
        public String prefix = "zeebe-record"; // prefix for cols
        public boolean createCollections = true; // creates cols (with indices) on startup

        // record types to export
        public boolean command = false;
        public boolean event = true;
        public boolean rejection = false;

        // value types to export
        public boolean deployment = true;
        public boolean error = true;
        public boolean incident = true;
        public boolean job = true;
        public boolean jobBatch = false;
        public boolean message = true;
        public boolean messageSubscription = true;
        public boolean variable = true;
        public boolean variableDocument = true;
        public boolean workflowInstance = true;
        public boolean workflowInstanceCreation = false;
        public boolean workflowInstanceSubscription = false;
        public boolean timers = true;
        // size limits
        public int ignoreVariablesAbove = 90000000;

        @Override
        public String toString() {
            return "ColConfiguration{"
                    + "prefix='"
                    + prefix
                    + '\''
                    + ", createCollections="
                    + createCollections
                    + ", command="
                    + command
                    + ", event="
                    + event
                    + ", rejection="
                    + rejection
                    + ", error="
                    + error
                    + ", deployment="
                    + deployment
                    + ", incident="
                    + incident
                    + ", job="
                    + job
                    + ", message="
                    + message
                    + ", messageSubscription="
                    + messageSubscription
                    + ", variable="
                    + variable
                    + ", variableDocument="
                    + variableDocument
                    + ", workflowInstance="
                    + workflowInstance
                    + ", workflowInstanceCreation="
                    + workflowInstanceCreation
                    + ", workflowInstanceSubscription="
                    + workflowInstanceSubscription
                    + ", timers="
                    + timers
                    + ", ignoreVariablesAbove="
                    + ignoreVariablesAbove
                    + '}';
        }

        public static class BulkConfiguration {
            // delay before forced flush
            public int delay = 5;
            // bulk size before flush
            public int size = 1_000;
            // memory limit of the bulk in bytes before flush
            public int memoryLimit = 10 * 1024 * 1024;

            @Override
            public String toString() {
                return "BulkConfiguration{"
                        + "delay="
                        + delay
                        + ", size="
                        + size
                        + ", memoryLimit="
                        + memoryLimit
                        + '}';
            }
        }
    }
}
