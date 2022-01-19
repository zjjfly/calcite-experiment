package com.github.zjjfly.ce.converter;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
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
        converters.put(SqlStdOperatorTable.EQUALS, l -> {
            SqlIdentifier identifier = (SqlIdentifier) l.get(0);
            String name = identifier.getSimple();
            SqlLiteral literal = (SqlLiteral) l.get(1);
            Parameter p = new Parameter();
            p.put(name, literal.getValue());
            return Collections.singletonList(p);
        });
        converters.put(SqlStdOperatorTable.OR, l -> {
            List<Parameter> result = Lists.newArrayList();
            l.forEach(sqlNode -> {
                if (sqlNode instanceof SqlBasicCall) {
                    result.addAll(convert((SqlBasicCall) sqlNode));
                }
            });
            return result;
        });
        converters.put(SqlStdOperatorTable.AND, l -> {
            List<Parameter> result = Lists.newArrayList();
            l.forEach(sqlNode -> {
                if (sqlNode instanceof SqlBasicCall) {
                    merge(result, convert((SqlBasicCall) sqlNode));
                }
            });
            return result;
        });
    }

    private void merge(List<Parameter> result, List<Parameter> convert) {
        if (result.isEmpty()) {
            result.addAll(convert);
            return;
        }
        result.forEach(parameter -> {
            convert.forEach(parameter::putAll);
        });
    }

    public void registerConverter(SqlOperator sqlOperator, ConverterFunc converterFunc) {
        converters.put(sqlOperator, converterFunc);
    }

    private List<Parameter> convert(SqlBasicCall call) {
        List<SqlNode> operandList = call.getOperandList();
        SqlOperator operator = call.getOperator();
        ConverterFunc converterFunc = converters.get(operator);
        if (converterFunc != null) {
            return converterFunc.apply(operandList);
        }
        return Lists.newArrayList();
    }

    public List<Parameter> convert(String condition, List<Parameter> parameters) {
        try {
            SqlBasicCall call = (SqlBasicCall) SqlParser.create(condition).parseExpression();
            return convert(call);
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        return parameters;
    }

    public static void main(String[] args) {
        String condition = "id = 1 or (id =2 and name ='x')";
        ConditionToParametersConverter converter = new ConditionToParametersConverter();
        List<Parameter> parameters = converter.convert(condition,
            Lists.newArrayList());
        System.out.println(parameters);
    }

}
