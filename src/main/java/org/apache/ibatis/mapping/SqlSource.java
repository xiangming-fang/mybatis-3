/*
 *    Copyright 2009-2023 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.mapping;

/**
 * Represents the content of a mapped statement read from an XML file or an annotation. It creates the SQL that will be
 * passed to the database out of the input parameter received from the user.
 *
 * @author Clinton Begin
 */
// Mybatis 会将 Mapper映射文件里的 sql语句解析成sqlsource对象
  // 动态sql标签 -> 一系列的sqlnode -> sqlsourcebuilder -> sqlsource
public interface SqlSource {

  // 控制着动态 SQL 语句解析的整个流程
  BoundSql getBoundSql(Object parameterObject);

}
