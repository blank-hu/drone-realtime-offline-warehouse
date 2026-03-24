package com.rtw.Demo;

import com.rtw.sink.DorisTable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.rtw.model.DwdTrajPoint;
import okhttp3.*;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.UUID;

public class DorisStreamLoadSink implements Sink<DwdTrajPoint>, Serializable {

    private final String feHost;
    private final int feHttpPort;
    private final String user;
    private final String password;
    private final DorisTable table;

    private final int maxRows;
    private final int maxBytes;

    public DorisStreamLoadSink(String feHost, int feHttpPort, String user, String password, DorisTable table) {
        this(feHost, feHttpPort, user, password, table, 5000, 5 * 1024 * 1024);
    }

    public DorisStreamLoadSink(String feHost, int feHttpPort, String user, String password,
                               DorisTable table, int maxRows, int maxBytes) {
        this.feHost = feHost;
        this.feHttpPort = feHttpPort;
        this.user = user;
        this.password = password == null ? "" : password;
        this.table = table;
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
    }

    /**
     * Flink 1.20 的 Sink 仍要求实现这个 deprecated 抽象方法，否则无法编译。:contentReference[oaicite:2]{index=2}
     */
    @SuppressWarnings("deprecation")
    public SinkWriter<DwdTrajPoint> createWriter(Sink.InitContext context) throws IOException {
        return new Writer(feHost, feHttpPort, user, password, table, maxRows, maxBytes, context.getSubtaskId());
    }

    static class Writer implements SinkWriter<DwdTrajPoint> {

        private static final ObjectMapper MAPPER = new ObjectMapper();
        private static final MediaType TEXT = MediaType.get("text/plain; charset=utf-8");

        private final String feHost;
        private final int feHttpPort;
        private final String user;
        private final String password;
        private final DorisTable table;
        private final int maxRows;
        private final int maxBytes;
        private final int subtaskId;

        private final OkHttpClient client = new OkHttpClient.Builder().build();
        private final StringBuilder buf = new StringBuilder();
        private int rows = 0;


        Writer(String feHost, int feHttpPort, String user, String password,
               DorisTable table, int maxRows, int maxBytes, int subtaskId) {
            this.feHost = feHost;
            this.feHttpPort = feHttpPort;
            this.user = user;
            this.password = password == null ? "" : password;
            this.table = table;
            this.maxRows = maxRows;
            this.maxBytes = maxBytes;
            this.subtaskId = subtaskId;
        }

        @Override
        public void write(DwdTrajPoint element, Context context) throws IOException, InterruptedException {
            buf.append(MAPPER.writeValueAsString(element)).append('\n');
            rows++;
            if (rows >= maxRows || buf.length() >= maxBytes) {
                flush(false);
            }
        }

        /**
         * SinkWriter 有 writeWatermark；我们不需要传递 watermarks，直接 no-op。:contentReference[oaicite:3]{index=3}
         */
        @Override
        public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
            // no-op
        }

        /**
         * flush 会在 checkpoint 或 end-of-input 时调用，用来实现 at-least-once 语义。:contentReference[oaicite:4]{index=4}
         */
        @Override
        public void flush(boolean endOfInput) throws IOException, InterruptedException {
            if (rows == 0) return;

            String url = table.url(feHost, feHttpPort);
            String label = "rtw_" + subtaskId + "_" + UUID.randomUUID();
            String auth = Base64.getEncoder()
                    .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));

            String payload = buf.toString();
            RequestBody body = RequestBody.create(payload, TEXT);

            Request req = new Request.Builder()
                    .url(url)
                    .addHeader("Authorization", "Basic " + auth)
                    .addHeader("label", label)
                    .addHeader("format", "json")
                    .addHeader("read_json_by_line", "true")
                    .addHeader("Expect", "100-continue")   // ✅ 关键修复
                    .put(body)
                    .build();

            String respBody = "";
            int code = -1;

            try (Response resp = client.newCall(req).execute()) {
                String bodyStr = resp.body() != null ? resp.body().string() : "";

                // 1) 先看 HTTP
                if (!resp.isSuccessful()) {
                    throw new RuntimeException("Doris stream load HTTP fail: code=" + resp.code() + ", body=" + bodyStr);
                }

                // 2) 再看 Doris 业务状态（关键）
                JsonNode node = MAPPER.readTree(bodyStr);
                String status = node.path("Status").asText(node.path("status").asText(""));

                if (!"Success".equalsIgnoreCase(status)) {
                    String msg = node.path("Message").asText(node.path("msg").asText(""));
                    String errorUrl = node.path("ErrorURL").asText(node.path("ErrorURL").asText(""));
                    throw new RuntimeException("Doris stream load FAIL: status=" + status
                            + ", msg=" + msg + ", errorUrl=" + errorUrl + ", raw=" + bodyStr);
                }
            }


            buf.setLength(0);
            rows = 0;
        }


        @Override
        public void close() throws Exception {
            flush(true);
        }
    }
}
