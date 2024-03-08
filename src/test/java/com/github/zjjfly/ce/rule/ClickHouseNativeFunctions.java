package com.github.zjjfly.ce.rule;

import com.github.zjjfly.ce.common.SqlNativeFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

public class ClickHouseNativeFunctions {

    /**
     * to_start_of_interval(time,INTERVAL 1 DAY)
     */
    public static final SqlOperator TO_START_OF_INTERVAL =
            new SqlNativeFunction("toStartOfInterval", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP,
                    null,
                    OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL));

    /**
     * timestamp_add(time,INTERVAL 1 DAY)
     */
    public static final SqlOperator TIMESTAMP_ADD =
            new SqlNativeFunction("timestamp_add", SqlKind.OTHER_FUNCTION, ReturnTypes.TIMESTAMP, null,
                    OperandTypes.family(SqlTypeFamily.DATETIME, SqlTypeFamily.DATETIME_INTERVAL));

}
