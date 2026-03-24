package com.rtw.serde;


import com.rtw.model.OdsTrajPoint;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;

import java.io.IOException;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TrajPointDeser implements DeserializationSchema<OdsTrajPoint> {
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    @Override
    public OdsTrajPoint deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, OdsTrajPoint.class);
    }

    @Override public boolean isEndOfStream(OdsTrajPoint nextElement) { return false; }

    @Override
    public TypeInformation<OdsTrajPoint> getProducedType() {
        return TypeInformation.of(OdsTrajPoint.class);
    }
}
