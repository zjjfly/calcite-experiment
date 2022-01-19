package com.github.zjjfly.ce.converter;

import java.util.List;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.sql.SqlNode;

/**
 * @author zijunjie[https://github.com/zjjfly]
 * @date 2022/1/18
 */
public interface ConverterFunc<T> extends
    Function2<List<SqlNode>, ExecutorFunc<T>, ExecutorFunc<T>> {

}
