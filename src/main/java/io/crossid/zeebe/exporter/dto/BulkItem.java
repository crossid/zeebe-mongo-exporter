package io.crossid.zeebe.exporter.dto;

//import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

//@JsonIgnoreProperties(ignoreUnknown = true)
public final class BulkItem {

    private BulkItemIndex index;

    public BulkItemIndex getIndex() {
        return index;
    }

    public void setIndex(final BulkItemIndex index) {
        this.index = index;
    }
}
