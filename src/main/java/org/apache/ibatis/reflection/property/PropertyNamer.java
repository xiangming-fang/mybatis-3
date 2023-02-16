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

import java.util.Locale;

import org.apache.ibatis.reflection.ReflectionException;

/**
 * @author Clinton Begin
 */
public final class PropertyNamer {

  private PropertyNamer() {
    // Prevent Instantiation of Static Class
  }

  // 根据方法名，属性名
  // eg：isTrue、setTure、getTrue 都是 true
  public static String methodToProperty(String name) {
    if (name.startsWith("is")) {
      name = name.substring(2);
    } else if (name.startsWith("get") || name.startsWith("set")) {
      name = name.substring(3);
    } else {
      throw new ReflectionException(
          "Error parsing property name '" + name + "'.  Didn't start with 'is', 'get' or 'set'.");
    }

    // 如果截取之后只有一个的话
    // 如果长度大于1，判断第二个字符是否是大写的，是大写的话说明也不是我们要的属性
    // 感觉是因为除了26个字母，不存在长度为1的单词
    if (name.length() == 1 || (name.length() > 1 && !Character.isUpperCase(name.charAt(1)))) {
      // toLowerCase，还是得按驼峰式的走
      name = name.substring(0, 1).toLowerCase(Locale.ENGLISH) + name.substring(1);
    }

    return name;
  }

  public static boolean isProperty(String name) {
    return isGetter(name) || isSetter(name);
  }

  // 判断一个方法是否是getter方法
  public static boolean isGetter(String name) {
    // 方法名以get开头，并且长度大于3
    // 或者 方法名以 is 开头，并且长度大于2
    return (name.startsWith("get") && name.length() > 3) || (name.startsWith("is") && name.length() > 2);
  }

  public static boolean isSetter(String name) {
    return name.startsWith("set") && name.length() > 3;
  }

}
