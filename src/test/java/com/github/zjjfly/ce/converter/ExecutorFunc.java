package com.github.zjjfly.ce.converter;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.function.Function1;

/**
 * @author zijunjie[https://github.com/zjjfly]
 * @date 2022/1/18
 */
public interface ExecutorFunc<T> extends Function1<Map<String, Object>, List<T>> {

  default ExecutorFunc<T> or(ExecutorFunc<T> executorFunc) {
    return m -> {
      List<T> result = Lists.newArrayList();
      result.addAll(executorFunc.apply(m));
      result.addAll(this.apply(m));
      return result;
    };
  }

  default ExecutorFunc<T> and(ExecutorFunc<T> executorFunc) {
    return m -> {
      ExecutorFunc<T> e = (map) -> {
        HashMap<String, Object> restoreMap = Maps.newHashMap();
        List<String> newKeys = Lists.newArrayList();
        map.forEach((k, v) -> {
          Object o = m.get(k);
          if (o != null) {
            restoreMap.put(k, o);
          } else {
            newKeys.add(k);
          }
          m.put(k, v);
        });
        List<T> result = executorFunc.apply(m);
        newKeys.forEach(m::remove);
        m.putAll(restoreMap);
        return result;
      };
      return e.apply(m);
    };
  }

  default <C> ExecutorFunc<T> in(String key, List<C> list) {
    return m -> {
      List<T> result = Lists.newArrayList();
      Object o = m.get(key);
      for (C c : list) {
        m.put(key, c);
        result.addAll(this.apply(m));
      }
      if (o != null) {
        m.put(key, o);
      } else {
        m.remove(key);
      }
      return result;
    };
  }

  default <C> ExecutorFunc<T> eq(String key, C c) {
    return m -> {
      List<T> result = Lists.newArrayList();
      Object o = m.get(key);
      m.put(key, c);
      result.addAll(this.apply(m));
      if (o != null) {
        m.put(key, o);
      } else {
        m.remove(key);
      }
      return result;
    };
  }

  default <C extends Comparable<?>> ExecutorFunc<T> range(String key, Range<C> range,
      DiscreteDomain<C> discreteDomain) {
    return m -> {
      List<T> result = Lists.newArrayList();
      Object o = m.get(key);
      for (C c : ContiguousSet.create(range, discreteDomain)) {
        m.put(key, c);
        result.addAll(this.apply(m));
      }
      if (o != null) {
        m.put(key, o);
      } else {
        m.remove(key);
      }
      return result;
    };
  }

}
