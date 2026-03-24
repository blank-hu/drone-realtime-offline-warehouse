package com.rtw.sink;

import java.io.Serializable;

public class DorisTable implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String db;
    public final String table;

    public DorisTable(String db, String table) {
        this.db = db;
        this.table = table;
    }

    public String url(String feHost, int feHttpPort) {
        return String.format("http://%s:%d/api/%s/%s/_stream_load", feHost, feHttpPort, db, table);
    }
}
