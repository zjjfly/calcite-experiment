package com.github.zjjfly.ce;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.google.common.collect.Lists;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

public class MySqlSqlWindowStartEnd implements WindowAggregationRule.SqlWindowStartEnd {

    /**
     * 构造类似下面的表达式 timestampadd(YEAR, floor(timestampdiff(YEAR, '1970-01-01', timeCol) / 3) * 3 ,
     * "1970-01-01")
     */
    @Override
    public RexNode windowStart(RelDataTypeFactory typeFactory, RexNode timeCol,
        RexLiteral interval) {
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        BigDecimal intervalValue = (BigDecimal) interval.getValue();
        SqlTypeName typeName = interval.getTypeName();
        IntervalSqlType intervalSqlType = (IntervalSqlType) interval.getType();
        SqlIntervalQualifier timeUnit = intervalSqlType.getIntervalQualifier();
        RexNode startTime = rexBuilder.makeLiteral("1970-01-01", typeFactory.createSqlType(
            SqlTypeName.VARCHAR), false);
        RexLiteral unit = rexBuilder.makeFlag(timeUnit.timeUnitRange.startUnit);
        RexNode itl = rexBuilder.makeLiteral(intervalValue.intValue(), typeFactory.createSqlType(
            SqlTypeName.INTEGER), true);
        if (typeName != SqlTypeName.INTERVAL_MONTH && typeName != SqlTypeName.INTERVAL_YEAR) {
            itl =
                rexBuilder.makeLiteral(
                    intervalValue.divide(BigDecimal.valueOf(1000), 2, RoundingMode.FLOOR)
                        .intValue(),
                    itl.getType());
            unit = rexBuilder.makeFlag(TimeUnit.SECOND);
        }
        if (typeName == SqlTypeName.INTERVAL_YEAR) {
            unit = rexBuilder.makeFlag(TimeUnit.MONTH);
        }
        RexNode timeDiff =
            rexBuilder.makeCall(SqlStdOperatorTable.TIMESTAMP_DIFF, unit, startTime, timeCol);
        //构造floor(timestampdiff(unit, '1970-01-01', timeCol) / itl) * itl
        RexNode intervalCount =
            rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
                rexBuilder.makeCall(SqlStdOperatorTable.FLOOR,
                    rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE,
                        Lists.newArrayList(timeDiff, itl))), itl);
        return rexBuilder.makeCall(getType(typeFactory), SqlStdOperatorTable.TIMESTAMP_ADD,
            Lists.newArrayList(unit, intervalCount, startTime));
    }

    /**
     * 构造类似下面的表达式 timestampadd(YEAR, floor(timestampdiff(YEAR, '1970-01-01', timeCol) / 3 + 1) * 3 ,
     * "1970-01-01")
     */
    @Override
    public RexNode windowEnd(RelDataTypeFactory typeFactory, RexNode timeCol, RexLiteral interval) {
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        BigDecimal intervalValue = (BigDecimal) interval.getValue();
        SqlTypeName typeName = interval.getTypeName();
        IntervalSqlType intervalSqlType = (IntervalSqlType) interval.getType();
        SqlIntervalQualifier timeUnit = intervalSqlType.getIntervalQualifier();
        RexNode startTime = rexBuilder.makeLiteral("1970-01-01", typeFactory.createSqlType(
            SqlTypeName.VARCHAR), false);
        RexLiteral unit = rexBuilder.makeFlag(timeUnit.timeUnitRange.startUnit);
        RelDataType integer = typeFactory.createSqlType(
            SqlTypeName.INTEGER);
        RexNode itl = rexBuilder.makeLiteral(intervalValue.intValue(), integer, true);
        if (typeName != SqlTypeName.INTERVAL_MONTH && typeName != SqlTypeName.INTERVAL_YEAR) {
            itl =
                rexBuilder.makeLiteral(
                    intervalValue.divide(BigDecimal.valueOf(1000), 2, RoundingMode.FLOOR)
                        .intValue(),
                    itl.getType());
            unit = rexBuilder.makeFlag(TimeUnit.SECOND);
        }
        if (typeName == SqlTypeName.INTERVAL_YEAR) {
            unit = rexBuilder.makeFlag(TimeUnit.MONTH);
        }
        RexNode timeDiff =
            rexBuilder.makeCall(SqlStdOperatorTable.TIMESTAMP_DIFF, unit, startTime, timeCol);
        //构造floor(timestampdiff(unit, '1970-01-01', timeCol) / itl + 1) * itl
        RexNode one = rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
            rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, Lists.newArrayList(timeDiff, itl)),
            rexBuilder.makeLiteral(1, integer));
        RexNode intervalCount =
            rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
                rexBuilder.makeCall(SqlStdOperatorTable.FLOOR,
                    one), itl);
        return rexBuilder.makeCall(getType(typeFactory), SqlStdOperatorTable.TIMESTAMP_ADD,
            Lists.newArrayList(unit, intervalCount, startTime));
    }
}
