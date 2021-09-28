package com.flink.streaming.core.udf;

import org.apache.flink.table.functions.ScalarFunction;

public class UDFSubstring extends ScalarFunction {
    private boolean endInclusive = false;

    public UDFSubstring(boolean endInclusive) {
        this.endInclusive = endInclusive;
    }

    public String eval(String s, Integer begin, Integer end) {
        return s.substring(begin, endInclusive ? end + 1 : end);
    }
}