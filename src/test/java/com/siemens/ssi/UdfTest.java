package com.siemens.ssi;

import java.sql.SQLException;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.junit.jupiter.api.Test;

@Slf4j
public class UdfTest extends CalciteTest {

  @Test
  public void udf() throws SQLException {
    String sql =
        "select u.id as user_id, u.name as user_name, j.company as user_company, u.age as user_age, square(u.id) as square "
            + "from ms.users u join ms.jobs j on u.name=j.name "
            + "where u.age > 30 and j.id>10 "
            + "order by user_id";

    schema.add("SQUARE", ScalarFunctionImpl.create(SquareFunction.class, "eval"));
    executeQuery(sql);
  }
}
