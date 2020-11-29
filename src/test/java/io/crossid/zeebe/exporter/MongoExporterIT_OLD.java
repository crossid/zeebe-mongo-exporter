package io.crossid.zeebe.exporter;

import io.zeebe.client.ZeebeClient;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.intent.WorkflowInstanceIntent;
import io.zeebe.test.ZeebeTestRule;
import io.zeebe.test.util.TestUtil;
import io.zeebe.test.util.record.RecordingExporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.zeebe.test.util.record.RecordingExporter.workflowInstanceRecords;

public class MongoExporterIT_OLD {
    private static final BpmnModelInstance WORKFLOW =
//            Bpmn.createExecutableProcess("testProcess")
//                    .startEvent("start")
//                    .sequenceFlowId("to-task")
//                    .serviceTask("task", s -> s.zeebeJobType("test"))
//                    .sequenceFlowId("to-end")
//                    .endEvent("end")
//                    .done();

            Bpmn.createExecutableProcess("testProcess")
                    .startEvent("start")
                    .intermediateCatchEvent(
                            "message", e -> e.message(m -> m.name("catch").zeebeCorrelationKey("=orderId")))
                    .serviceTask("task", t -> t.zeebeJobType("work").zeebeTaskHeader("foo", "bar"))
                    .endEvent()
                    .done();


    public static final BpmnModelInstance SECOND_WORKFLOW =
            Bpmn.createExecutableProcess("secondProcess").startEvent().endEvent().done();

    private static final MongoExporterConfiguration CONFIGURATION = new MongoExporterConfiguration();

    @Rule
    public final ZeebeTestRule
            testRule = new ZeebeTestRule();

    private ZeebeClient client;

    @Before
    public void init() {
        client = testRule.getClient();
        // init mongo client
//        final ClientConfig clientConfig = new ClientConfig();
//        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5701");
    }

    @After
    public void cleanUp() {
//        mclient.shutdown();
    }

    @Test
    public void shouldExportRecords() throws Exception {
        final String orderId = "foo-bar-123";

        // deploy workflow
        client
                .newDeployCommand()
                .addWorkflowModel(WORKFLOW, "workflow.bpmn")
//                .addWorkflowModel(SECOND_WORKFLOW, "secondWorkflow.bpmn")
                .send()
                .join();

        client
                .newCreateInstanceCommand()
                .bpmnProcessId("testProcess")
                .latestVersion()
                .variables(Collections.singletonMap("orderId", orderId))
                .send()
                .join();

        // create job worker which fails on first try and sets retries to 0 to create an incident
        final AtomicBoolean fail = new AtomicBoolean(true);

        client.newWorker()
                .jobType("work")
                .handler(
                        (jobClient, job) -> {
                            jobClient.newCompleteCommand(job.getKey()).send().join();
                        })
                .open();

        // publish message to trigger message catch event
        client
                .newPublishMessageCommand()
                .messageName("catch")
                .correlationKey(orderId)
                .send()
                .join();

        // wait until workflow instance is completed
        TestUtil.waitUntil(
                () ->
                        workflowInstanceRecords(WorkflowInstanceIntent.ELEMENT_COMPLETED)
                                .filter(r -> r.getKey() == r.getValue().getWorkflowInstanceKey())
                                .exists());

        // assert all records which where recorded during the tests where exported
        assertRecordsExported();
    }

    private void assertRecordsExported() {
        RecordingExporter.getRecords().forEach(this::assertRecordExported);
    }

    private void assertRecordExported(Record<?> record) {
//        final Map<String, Object> source = esClient.get(record);
//        assertThat(source)
//                .withFailMessage("Failed to fetch record %s from elasticsearch", record)
//                .isNotNull();
//        assertThat(source).isEqualTo(recordToMap(record));
    }
}
