package com.github.zjjfly.ce.converter;

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
public class ConditionToParametersConverter<T> {

  private final Map<SqlOperator, ConverterFunc<T>> converters = Maps.newHashMap();

  public ConditionToParametersConverter() {
    converters.put(SqlStdOperatorTable.EQUALS, (sqlNodeList, executorFunc) -> {
      SqlNode sqlNode = sqlNodeList.get(0);
      SqlIdentifier identifier = (SqlIdentifier) sqlNode;
      String name = identifier.getSimple();
      SqlLiteral literal = (SqlLiteral) sqlNodeList.get(1);
      Object value = literal.getValue();
      return executorFunc.eq(name, value);
    });
    converters.put(SqlStdOperatorTable.OR, (sqlNodeList, executorFunc) -> {
      for (SqlNode sqlNode : sqlNodeList) {
        executorFunc = executorFunc.or(convert((SqlBasicCall) sqlNode, executorFunc));
      }
      return executorFunc;
    });
    converters.put(SqlStdOperatorTable.AND, (sqlNodeList, executorFunc) -> {
      for (SqlNode sqlNode : sqlNodeList) {
        executorFunc = executorFunc.and(convert((SqlBasicCall) sqlNode, executorFunc));
      }
      return executorFunc;
    });
  }

  public ExecutorFunc<T> convert(SqlBasicCall call, ExecutorFunc<T> executorFunc) {
    List<SqlNode> operandList = call.getOperandList();
    SqlOperator operator = call.getOperator();
    ConverterFunc<T> converterFunc = converters.get(operator);
    if (converterFunc != null) {
      return converterFunc.apply(operandList, executorFunc);
    }
    return null;
  }

  public static void main(String[] args) throws SqlParseException {
    SqlNode sqlNode = SqlParser.create("name = 'xxx' or id = 2 ")
        .parseExpression();
    ConditionToParametersConverter<String> converter = new ConditionToParametersConverter<>();
    ExecutorFunc<String> executorFunc = converter.convert((SqlBasicCall) sqlNode,
        m -> {
          System.out.println(m);
          return Collections.singletonList(m.toString());
        });
    List<String> strings = executorFunc.apply(Maps.newHashMap());
    System.out.println(strings);
  }

}
