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

import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SystemTransactionOffsetTest {
  private final Timestamp ts = new Timestamp(100L);
  private final long id = 1000L;
  private final SystemTransactionOffset unset = new SystemTransactionOffset(null);
//  private final TimestampIncrementingOffset tsOnly = new TimestampIncrementingOffset(ts, null);
//  private final TimestampIncrementingOffset incOnly = new TimestampIncrementingOffset(null, id);
  private final SystemTransactionOffset tsInc = new SystemTransactionOffset(id);
//  private Timestamp nanos;
//  private SystemTransactionOffset nanosOffset;

//  @Before
//  public void setUp() {
//    long millis = System.currentTimeMillis();
//    nanos = new Timestamp(millis);
//    nanos.setNanos((int)(millis % 1000) * 1000000 + 123456);
//    assertEquals(millis, nanos.getTime());
//    nanosOffset = new TimestampIncrementingOffset(nanos, null);
//  }

  @Test
  public void testDefaults() {
    assertEquals(-1, unset.getIncrementingOffset());
    assertNotNull(unset.getIncrementingOffset());
//    assertEquals(0, unset.getTimestampOffset().getTime());
//    assertEquals(0, unset.getTimestampOffset().getNanos());
  }

  @Test
  public void testToMap() {
    assertEquals(0, unset.toMap().size());
//    assertEquals(2, tsOnly.toMap().size());
//    assertEquals(1, incOnly.toMap().size());
    assertEquals(3, tsInc.toMap().size());
//    assertEquals(2, nanosOffset.toMap().size());
  }

  @Test
  public void testGetIncrementingOffset() {
    assertEquals(-1, unset.getIncrementingOffset());
//    assertEquals(-1, tsOnly.getIncrementingOffset());
//    assertEquals(id, incOnly.getIncrementingOffset());
    assertEquals(id, tsInc.getIncrementingOffset());
//    assertEquals(-1, nanosOffset.getIncrementingOffset());
  }

//  @Test
//  public void testGetTimestampOffset() {
//    assertNotNull(unset.getTimestampOffset());
//    Timestamp zero = new Timestamp(0);
//    assertEquals(zero, unset.getTimestampOffset());
//    assertEquals(ts, tsOnly.getTimestampOffset());
//    assertEquals(zero, incOnly.getTimestampOffset());
//    assertEquals(ts, tsInc.getTimestampOffset());
//    assertEquals(nanos, nanosOffset.getTimestampOffset());
//  }

  @Test
  public void testFromMap() {
    assertEquals(unset, TimestampIncrementingOffset.fromMap(unset.toMap()));
//    assertEquals(tsOnly, TimestampIncrementingOffset.fromMap(tsOnly.toMap()));
//    assertEquals(incOnly, TimestampIncrementingOffset.fromMap(incOnly.toMap()));
    assertEquals(tsInc, TimestampIncrementingOffset.fromMap(tsInc.toMap()));
//    assertEquals(nanosOffset, TimestampIncrementingOffset.fromMap(nanosOffset.toMap()));
  }

  @Test
  public void testEquals() {
//    assertEquals(nanosOffset, nanosOffset);
    assertEquals(new SystemTransactionOffset(null), new SystemTransactionOffset(null));
    assertEquals(unset, new SystemTransactionOffset(null));

//    TimestampIncrementingOffset x = new TimestampIncrementingOffset(null, id);
//    assertEquals(x, incOnly);

//    x = new TimestampIncrementingOffset(ts, null);
//    assertEquals(x, tsOnly);

    SystemTransactionOffset x = new SystemTransactionOffset(id);
    assertEquals(x, tsInc);

//    x = new TimestampIncrementingOffset(nanos, null);
//    assertEquals(x, nanosOffset);
  }

}
