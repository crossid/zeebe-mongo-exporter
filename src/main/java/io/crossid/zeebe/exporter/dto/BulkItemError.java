package io.crossid.zeebe.exporter.dto;

public final class BulkItemError {

    private String type = "";
    private String reason = "";

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(final String reason) {
        this.reason = reason;
    }
}
