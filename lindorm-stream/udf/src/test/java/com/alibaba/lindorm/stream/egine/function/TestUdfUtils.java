/*
 * Licensed to Lindorm under one or more contributor license
 * agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. Lindorm
 * licenses this file to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.lindorm.stream.egine.function;

import com.alibaba.lindorm.stream.engine.function.UdfUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.kafka.connect.data.Schema.*;
import static org.junit.Assert.fail;

public class TestUdfUtils {

  @Test
  public void testToSchema(){
    Assert.assertEquals(STRING_SCHEMA, UdfUtils.toSchema("String"));
    Assert.assertEquals(INT8_SCHEMA, UdfUtils.toSchema("INT8"));
    Assert.assertEquals(INT16_SCHEMA, UdfUtils.toSchema("INT16"));
    Assert.assertEquals(INT32_SCHEMA, UdfUtils.toSchema("INT32"));
    Assert.assertEquals(FLOAT32_SCHEMA, UdfUtils.toSchema("FLOAT32"));
    Assert.assertEquals(FLOAT64_SCHEMA, UdfUtils.toSchema("FLOAT64"));
    Assert.assertEquals(BOOLEAN_SCHEMA, UdfUtils.toSchema("BOOLEAN"));
    Assert.assertEquals(BYTES_SCHEMA, UdfUtils.toSchema("BYTES"));
    try {
      Assert.assertEquals(BYTES_SCHEMA, UdfUtils.toSchema("map"));
      fail();
    } catch (Exception e) {
      Assert.assertTrue(e.getMessage().contains("unKnow schema type"));
    }
  }
}
