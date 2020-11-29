package io.crossid.zeebe.exporter.util;

public interface MongoNode<SELF extends MongoNode> {

    void start();

    void stop();

    String getUrl();

    SELF withPort(int port);
}
