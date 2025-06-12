package org.apache.beam.runners.flink.translation.utils;

import java.util.List;
import org.apache.beam.runners.flink.FlinkBatchPortablePipelineTranslator;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * [Glean]
 * FilterFunction that filters out large records based on a size threshold.
 */
public class LargeRecordFilterFunction<K, V> implements
    FilterFunction<WindowedValue<KV<K, V>>> {
  private static final Logger LOG =
      LoggerFactory.getLogger(LargeRecordFilterFunction.class);
  private static final long MAX_RECORD_SIZE =  5000000; // 5 MB

  @Override
  public boolean filter(WindowedValue<KV<K, V>> windowedValue) throws Exception {
    KV<K, V> kv = windowedValue.getValue();
    long size = getObjectSize(kv.getKey()) + getObjectSize(kv.getValue());
    if (size >= MAX_RECORD_SIZE) {
      LOG.warn("Dropping large record with size: {}", size);
      return false;
    }
    return true;
  }

  /**
   * Calculate the size of an object in bytes.
   * This is a simplified version for objects used in portability.
   */
  private static <T> long getObjectSize(T o) {
    if (o instanceof byte[]) {
      return ((byte[]) o).length;
    } else if(o instanceof List) {
      return ((List<?>) o).stream().mapToLong(LargeRecordFilterFunction::getObjectSize).sum();
    } else {
      return 0; // for other types, we don't calculate size
    }
  }
}
