package com.github.zjjfly.ce.converter;

import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

/**
 * @author zijunjie[https://github.com/zjjfly]
 * @date 2022/1/18
 */
public interface ConverterFunc extends Function1<List<SqlNode>, List<Parameter>> {

}
