/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class SystemTransactionOffset {
  private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

  private static final String INCREMENTING_FIELD = "system_transaction";

  private final Long incrementingOffset;

  /**
   * @param incrementingOffset the incrementing offset.
   *                           If null, {@link #getIncrementingOffset()} will return -1.
   */
  public SystemTransactionOffset(Long incrementingOffset) {
    this.incrementingOffset = incrementingOffset;
  }

  public long getIncrementingOffset() {
    return incrementingOffset == null ? -1 : incrementingOffset;
  }

  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<>(1);
    if (incrementingOffset != null) {
      map.put(INCREMENTING_FIELD, incrementingOffset);
    }
    return map;
  }

  public static SystemTransactionOffset fromMap(Map<String, ?> map) {
    if (map == null || map.isEmpty()) {
      return new SystemTransactionOffset(null);
    }

    Long incr = (Long) map.get(INCREMENTING_FIELD);
    return new SystemTransactionOffset(incr);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SystemTransactionOffset that = (SystemTransactionOffset) o;

    if (incrementingOffset != null
        ? !incrementingOffset.equals(that.incrementingOffset)
        : that.incrementingOffset != null) {
      return false;
    }
    return true;

  }

  @Override
  public int hashCode() {
    int result = incrementingOffset != null ? incrementingOffset.hashCode() : 0;
    return result;
  }
}
