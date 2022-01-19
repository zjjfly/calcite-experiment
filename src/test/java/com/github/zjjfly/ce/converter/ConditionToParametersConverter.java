package com.github.zjjfly.ce.converter;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * @author Zi JunJie(junjie.zi@siemens.com)
 */
public class ConditionToParametersConverter {

    private final Map<SqlOperator, ConverterFunc> converters = Maps.newHashMap();

    public ConditionToParametersConverter() {
        converters.put(SqlStdOperatorTable.EQUALS, (l, m) -> m);
        converters.put(SqlStdOperatorTable.OR, (l, m) -> m);
        converters.put(SqlStdOperatorTable.AND, (l, m) -> m);
    }

    public void registerConverter(SqlOperator sqlOperator, ConverterFunc converterFunc) {
        converters.put(sqlOperator, converterFunc);
    }

    public List<Parameter> convert(String condition, List<Parameter> parameters) {
        try {
            SqlBasicCall call = (SqlBasicCall) SqlParser.create(condition).parseExpression();
            List<SqlNode> operandList = call.getOperandList();
            SqlOperator operator = call.getOperator();
            ConverterFunc converterFunc = converters.get(operator);
            if (converterFunc != null) {
                return converterFunc.apply(operandList, parameters);
            }
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        return parameters;
    }

}
