/*
 *    Copyright 2009-2022 the original author or authors.
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
package org.apache.ibatis.cache.decorators;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.ibatis.cache.Cache;

/**
 * Lru (least recently used) cache decorator.
 *
 * @author Clinton Begin
 */
// lru缓存淘汰策略：淘汰最近最少使用的缓存条目
public class LruCache implements Cache {
  // 被装饰的cache对象
  private final Cache delegate;
  // 存储缓存key值的
  private Map<Object, Object> keyMap;
  // 待移除的key值
  private Object eldestKey;

  public LruCache(Cache delegate) {
    this.delegate = delegate;
    setSize(1024);
  }

  @Override
  public String getId() {
    return delegate.getId();
  }

  @Override
  public int getSize() {
    return delegate.getSize();
  }

  public void setSize(final int size) {
    // 用linkedHashMap存储缓存的key，双链表
    // 并且重写 removeEldestEntry => 是否移除最老的key值
    keyMap = new LinkedHashMap<Object, Object>(size, .75F, true) {
      private static final long serialVersionUID = 4267176411845948333L;

      // 返回true就是要清除某个条目
      @Override
      protected boolean removeEldestEntry(Map.Entry<Object, Object> eldest) {
        // 当keymap的当前size 大于 初始size时就要淘汰某个条目了
        boolean tooBig = size() > size;
        if (tooBig) {
          eldestKey = eldest.getKey();
        }
        return tooBig;
      }
    };
  }

  @Override
  public void putObject(Object key, Object value) {
    delegate.putObject(key, value);
    // 维护当前对象的成员变量
    cycleKeyList(key);
  }

  @Override
  public Object getObject(Object key) {
    // 这步操作至关重要，虽然没用使用到它的返回值
    // 但是我们这里的淘汰策略是LRU：最近未使用的
    // 我们的keymap是双链表结构（构建的时候有accessOrder为true）,每个节点是entry类型的
    // 这里我们使用了这个key缓存，那么我们就把key对应的entry移动到keymap这个双链表的末尾（这个操作是底层实现的）
    // 每次使用的时候都进行这样一个操作，那么我们要淘汰的最老节点就是我们的头结点
    keyMap.get(key); // touch
    return delegate.getObject(key);
  }

  @Override
  public Object removeObject(Object key) {
    return delegate.removeObject(key);
  }

  @Override
  public void clear() {
    delegate.clear();
    keyMap.clear();
  }

  // 这才是我们要的淘汰策略
  private void cycleKeyList(Object key) {
    keyMap.put(key, key);
    // 如果有最年长的key，那么就得淘汰它
    if (eldestKey != null) {
      delegate.removeObject(eldestKey);
      // 重新将最年长的置为null
      eldestKey = null;
    }
  }

}
