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
package org.apache.ibatis.reflection.property;

import java.util.Iterator;

/**
 * @author Clinton Begin
 */
// 属性解析工具：负责解析 ”.“ 和 ”[]“ 构成的表达式
// 实现 iterator 接口 可以迭代处理嵌套多层的表达式
public class PropertyTokenizer implements Iterator<PropertyTokenizer> {

  private String name;
  private final String indexedName;
  private String index;
  private final String children;

  // 假设待解析字符串是：countrys[0].provinces[0].cities[0]
  public PropertyTokenizer(String fullname) {
    int delim = fullname.indexOf('.');
    // 说明是多层的，存在孩子
    if (delim > -1) {
      // name = countrys[0]
      name = fullname.substring(0, delim);
      // delim + 1 跳过 ‘.'
      // children = provinces[0].cities[0]
      children = fullname.substring(delim + 1);
    }
    // 只有一层，一个属性名
    else {
      name = fullname;
      children = null;
    }
    // indexedName = countrys[0]
    indexedName = name;
    // 找到对应数组名字和数组下标
    delim = name.indexOf('[');
    if (delim > -1) {
      // index = 0
      index = name.substring(delim + 1, name.length() - 1);
      // name = countrys
      name = name.substring(0, delim);
    }
  }

  public String getName() {
    return name;
  }

  public String getIndex() {
    return index;
  }

  public String getIndexedName() {
    return indexedName;
  }

  public String getChildren() {
    return children;
  }

  @Override
  public boolean hasNext() {
    return children != null;
  }

  @Override
  public PropertyTokenizer next() {
    return new PropertyTokenizer(children);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException(
        "Remove is not supported, as it has no meaning in the context of properties.");
  }
}
