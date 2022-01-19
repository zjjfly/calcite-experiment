package com.github.zjjfly.ce.converter;

import java.util.List;

import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.sql.SqlNode;

/**
 * @author zijunjie[https://github.com/zjjfly]
 * @date 2022/1/18
 */
public interface ConverterFunc extends Function2<List<SqlNode>, List<Parameter>, List<Parameter>> {

}
