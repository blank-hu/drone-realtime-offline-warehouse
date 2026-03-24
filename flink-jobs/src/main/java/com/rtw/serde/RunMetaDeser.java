package com.rtw.serde;

import com.rtw.model.OdsRunMeta;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;

import java.io.IOException;

/** 忽略未知字段，避免 producer 字段演进导致反序列化失败 */
public class RunMetaDeser implements DeserializationSchema<OdsRunMeta> {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Override
    public OdsRunMeta deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, OdsRunMeta.class);
    }

    @Override public boolean isEndOfStream(OdsRunMeta nextElement) { return false; }

    @Override
    public TypeInformation<OdsRunMeta> getProducedType() {
        return TypeInformation.of(OdsRunMeta.class);
    }
}
