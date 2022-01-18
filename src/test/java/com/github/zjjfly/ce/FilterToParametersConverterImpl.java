package com.github.zjjfly.ce;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * @author Zi JunJie(junjie.zi@siemens.com)
 */
public class FilterToParametersConverterImpl implements FilterConverter<List<Parameter>> {

    @Override
    public List<Parameter> convert(SqlNode sqlNode) {
        ArrayList<Parameter> result = Lists.newArrayList();
        parse(sqlNode, result);
        return result;
    }

    private void parse(SqlNode sqlNode, List<Parameter> parameters) {
        if (sqlNode instanceof SqlCall) {

        }
    }

    public static void main(String[] args) throws SqlParseException {
        SqlNode sqlNode = SqlParser.create("name = 'xx' and id in (2,3)").parseExpression();
        FilterToParametersConverterImpl filterToParametersConverter =
            new FilterToParametersConverterImpl();
        List<Parameter> parameters = filterToParametersConverter.convert(sqlNode);
        System.out.println(parameters);
    }
}
