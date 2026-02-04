/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.flink.translation.utils;

import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** [Glean] FilterFunction that filters out large records based on a size threshold. */
public class LargeRecordFilterFunction<K, V> implements FilterFunction<WindowedValue<KV<K, V>>> {
  private static final Logger LOG = LoggerFactory.getLogger(LargeRecordFilterFunction.class);
  private static final long MAX_RECORD_SIZE = 5000000; // 5 MB

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
   * Calculate the size of an object in bytes. This is a simplified version for objects used in
   * portability.
   */
  private static <T> long getObjectSize(T o) {
    if (o instanceof byte[]) {
      return ((byte[]) o).length;
    } else if (o instanceof List) {
      return ((List<?>) o).stream().mapToLong(LargeRecordFilterFunction::getObjectSize).sum();
    } else {
      return 0; // for other types, we don't calculate size
    }
  }
}
