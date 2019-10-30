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

import org.junit.Test;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SystemTransactionOffsetTest {
  private final Timestamp ts = new Timestamp(100L);
  private final long id = 1000L;
  private final SystemTransactionOffset unset = new SystemTransactionOffset(null);
  private final SystemTransactionOffset tsInc = new SystemTransactionOffset(id);

  @Test
  public void testDefaults() {
    assertEquals(-1, unset.getIncrementingOffset());
    assertNotNull(unset.getIncrementingOffset());
  }

  @Test
  public void testToMap() {
    assertEquals(0, unset.toMap().size());
//    assertEquals(2, tsOnly.toMap().size());
//    assertEquals(1, incOnly.toMap().size());
    assertEquals(1, tsInc.toMap().size());
//    assertEquals(2, nanosOffset.toMap().size());
  }

  @Test
  public void testGetIncrementingOffset() {
    assertEquals(-1, unset.getIncrementingOffset());
    assertEquals(id, tsInc.getIncrementingOffset());
  }


  @Test
  public void testFromMap() {
    assertEquals(unset, SystemTransactionOffset.fromMap(unset.toMap()));
    assertEquals(tsInc, SystemTransactionOffset.fromMap(tsInc.toMap()));
  }

  @Test
  public void testEquals() {
    assertEquals(new SystemTransactionOffset(null), new SystemTransactionOffset(null));
    assertEquals(unset, new SystemTransactionOffset(null));

    SystemTransactionOffset x = new SystemTransactionOffset(id);
    assertEquals(x, tsInc);

  }

}
