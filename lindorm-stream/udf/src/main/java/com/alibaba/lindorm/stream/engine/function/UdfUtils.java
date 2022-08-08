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
package com.alibaba.lindorm.stream.engine.function;

import org.apache.kafka.connect.data.Schema;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.connect.data.Schema.BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.BYTES_SCHEMA;
import static org.apache.kafka.connect.data.Schema.FLOAT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.FLOAT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT16_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT32_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.INT8_SCHEMA;
import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.Type;

public class UdfUtils {

  /**
   * Parse schema from string
   * @param schemaStr
   * @return
   */
  public static Schema toSchema(String schemaStr){
    switch (Type.valueOf(schemaStr.trim().toUpperCase())) {
      case STRING:
        return STRING_SCHEMA;
      case INT8:
        return INT8_SCHEMA;
      case INT16:
        return INT16_SCHEMA;
      case INT32:
        return INT32_SCHEMA;
      case INT64:
        return INT64_SCHEMA;
      case FLOAT32:
        return FLOAT32_SCHEMA;
      case FLOAT64:
        return FLOAT64_SCHEMA;
      case BOOLEAN:
        return BOOLEAN_SCHEMA;
      case BYTES:
        return BYTES_SCHEMA;
      default:
        throw new IllegalStateException("unKnow schema type " + schemaStr);
    }
  }

  /**
   * Convert schema string to Schema List
   * @param schemaStr : separate by "," 。 eg. String,Boolean
   * @return
   */
  public static List<Schema> toSchemaList(String schemaStr){
    String[] schemaStrs = schemaStr.split(",");
    List<Schema> schemas = new ArrayList<>();
    for (String schema : schemaStrs) {
      schemas.add(toSchema(schema));
    }
    return schemas;
  }

  /**
   * Gen md5 string
   * @param str
   * @return
   */
  public static String getMD5Str(String str) {
    byte[] digest = null;
    try {
      MessageDigest md5 = MessageDigest.getInstance("md5");
      digest  = md5.digest(str.getBytes("utf-8"));
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
    }
    //16是表示转换为16进制数
    String md5Str = new BigInteger(1, digest).toString(16);
    return md5Str;
  }

  /**
   *  Convert path to URL[]
   * @param path
   * @return
   */
  public static URL[] toUrl(final Path path) {
    try {
      return new URL[]{path.toUri().toURL()};
    } catch (final MalformedURLException e) {
      throw new IllegalStateException("Unable to create classloader for path:" + path, e);
    }
  }

}
