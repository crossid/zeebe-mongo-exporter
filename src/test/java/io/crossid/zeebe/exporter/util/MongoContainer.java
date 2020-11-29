package io.crossid.zeebe.exporter.util;

import com.mongodb.client.MongoClient;
import org.testcontainers.containers.GenericContainer;

public class MongoContainer extends GenericContainer<MongoContainer>
        implements MongoNode<MongoContainer> {
    private static final int DEFAULT_PORT = 27017;
    private static final String DEFAULT_IMAGE = "mongo";
    private static final String DEFAULT_IMAGE_VER = "latest";
    private MongoClient mongoClient;
    private int port;

    public MongoContainer() {
        this(DEFAULT_IMAGE_VER);
    }

    public MongoContainer(final String version) {
        super(DEFAULT_IMAGE + ":" + version);
    }

    @Override
    public MongoContainer withPort(final int port) {
        this.port = port;
        addFixedExposedPort(port, DEFAULT_PORT);
        return this;
    }

    @Override
    protected void doStart() {
        super.doStart();
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public String getUrl() {
        return "mongodb://localhost:" + port;
    }
}
