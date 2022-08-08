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
package com.alibaba.lindorm.stream.engine.function.udf;

import com.alibaba.lindorm.stream.engine.function.UdfFunction;
import com.alibaba.lindorm.stream.engine.function.UdfUtils;

/**
 * Get md5sum of string
 */
@UdfAnnotation(name = "ld_md5", description = "example udf")
public class Md5Udf implements Kudf {

  @Override public void init() {
  }

  @UdfFunction(returnType = "string", arguments = "string")
  @Override public Object evaluate(Object... args) {
    if (args.length != 1) {
      throw new IllegalArgumentException("Md5 udf should have one input argument.");
    }
    return UdfUtils.getMD5Str((String) args[0]);
  }

}
