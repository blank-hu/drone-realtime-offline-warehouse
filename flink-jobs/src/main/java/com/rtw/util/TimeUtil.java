package com.rtw.util;

import java.time.*;
import java.time.format.DateTimeFormatter;

/** 规则固化：dt 永远从 t_ms 推导，且统一使用 UTC。 */
public class TimeUtil {
    private static final ZoneId UTC = ZoneOffset.UTC;
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE; // yyyy-MM-dd

    public static String dtUtc(long tMs) {
        return Instant.ofEpochMilli(tMs).atZone(UTC).toLocalDate().format(DATE_FMT);
    }
}
