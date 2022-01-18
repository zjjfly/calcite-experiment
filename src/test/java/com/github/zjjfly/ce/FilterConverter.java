package com.github.zjjfly.ce;

import org.apache.calcite.sql.SqlNode;

/**
 * @author Zi JunJie(junjie.zi@siemens.com)
 */
public interface FilterConverter<T> {

    T convert(SqlNode sqlNode);

}
