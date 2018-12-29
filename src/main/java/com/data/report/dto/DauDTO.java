package com.data.report.dto;

public class DauDTO {

    private long count;
    private String key;

    public DauDTO(long count, String key){
        this.count = count;
        this.key = key;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }
}
