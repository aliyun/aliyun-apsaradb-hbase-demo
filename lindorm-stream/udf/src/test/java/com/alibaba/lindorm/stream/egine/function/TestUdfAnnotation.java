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

import com.alibaba.lindorm.stream.engine.function.UdfFunction;
import com.alibaba.lindorm.stream.engine.function.UdfUtils;
import com.alibaba.lindorm.stream.engine.function.udf.Kudf;
import com.alibaba.lindorm.stream.engine.function.udf.UdfAnnotation;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.apache.kafka.connect.data.Schema.*;

public class TestUdfAnnotation {

  @Test
  public void testUdfAnnotation(){
    final UdfAnnotation udfDescriptionAnnotation = TestUdf.class.getAnnotation(UdfAnnotation.class);
    System.out.println(udfDescriptionAnnotation.name() + " " + udfDescriptionAnnotation.description());
    for (final Method method : TestUdf.class.getMethods()) {
      final UdfFunction udfFunction = method.getAnnotation(UdfFunction.class);
      if (udfFunction == null) {
        continue;
      }
      String udfName = udfDescriptionAnnotation.name();
      String returnTypeStr = udfFunction.returnType();
      String argsStr = udfFunction.arguments();
      System.out.println("udfName " + udfName);
      System.out.println("returnTypeStr " + returnTypeStr);
      System.out.println("argsStr " + argsStr);
      Assert.assertEquals(STRING_SCHEMA, UdfUtils.toSchema(returnTypeStr));
      String[] args = argsStr.split(",");
      Assert.assertEquals(2, args.length);
      Assert.assertEquals(STRING_SCHEMA, UdfUtils.toSchema(args[0]));
      Assert.assertEquals(BOOLEAN_SCHEMA, UdfUtils.toSchema(args[1]));
    }

  }

  @UdfAnnotation(name = "test_udf", description = "for test")
  public static class TestUdf implements Kudf {

    @Override public void init() {
    }

    @UdfFunction(returnType = "String", arguments = "String,boolean")
    @Override
    public Object evaluate(Object... args) {
      return null;
    }
  }

}


