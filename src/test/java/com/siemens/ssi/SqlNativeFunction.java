package com.siemens.ssi;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 *
 */
public class SqlNativeFunction extends SqlFunction {

  public SqlNativeFunction(String name, SqlKind kind,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker) {
    super(name, kind, returnTypeInference, operandTypeInference, operandTypeChecker, SqlFunctionCategory.SYSTEM);
  }

  public SqlNativeFunction(SqlIdentifier sqlIdentifier,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      @Nullable List<RelDataType> paramTypes) {
    super(sqlIdentifier, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes,
        SqlFunctionCategory.SYSTEM);
  }

  protected SqlNativeFunction(String name,
      @Nullable SqlIdentifier sqlIdentifier, SqlKind kind,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker,
      @Nullable List<RelDataType> paramTypes) {
    super(name, sqlIdentifier, kind, returnTypeInference, operandTypeInference, operandTypeChecker, paramTypes,
        SqlFunctionCategory.SYSTEM);
  }

  protected SqlNativeFunction(String name,
      @Nullable SqlIdentifier sqlIdentifier, SqlKind kind,
      @Nullable SqlReturnTypeInference returnTypeInference,
      @Nullable SqlOperandTypeInference operandTypeInference,
      @Nullable SqlOperandTypeChecker operandTypeChecker) {
    super(name, sqlIdentifier, kind, returnTypeInference, operandTypeInference, operandTypeChecker,
        SqlFunctionCategory.SYSTEM);
  }

  @Override
  public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    writer.print(this.getName());
    SqlWriter.Frame frame = writer.startList("(", ")");
    List<SqlNode> operandList = call.getOperandList();
    for (int i = 0, operandListSize = operandList.size(); i < operandListSize; i++) {
      SqlNode sqlNode = operandList.get(i);
      sqlNode.unparse(writer, 0, 0);
      if (i != operandListSize - 1) {
        writer.print(",");
      }
    }
    writer.endList(frame);
  }
}
