package com.siemens.ssi;

import com.mysql.cj.jdbc.Driver;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import ru.yandex.clickhouse.ClickHouseDriver;

@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
public abstract class CalciteTest {

  protected CalciteConnection calciteConnection;

  protected SchemaPlus schema;

  @BeforeAll
  public void init() throws SQLException {
    Properties info = new Properties();
    info.put(CalciteConnectionProperty.CASE_SENSITIVE.name(), Boolean.FALSE.toString());
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    calciteConnection = connection.unwrap(CalciteConnection.class);
    schema = calciteConnection.getRootSchema();
    JdbcSchema mysqlSchema = JdbcSchema.create(schema, "ms",
        JdbcSchema.dataSource("jdbc:mysql://localhost:13306/test", Driver.class.getName(), "root", "123456"),
        null, null);
    schema.add("ms", mysqlSchema);
    JdbcSchema clickHouseSchema = JdbcSchema.create(schema, "ch",
        JdbcSchema.dataSource(
            "jdbc:clickhouse://localhost:18123?use_server_time_zone=false&use_time_zone=Asia/Shanghai",
            ClickHouseDriver.class.getName(), "default", ""),
        null, null);
    schema.add("ch", clickHouseSchema);
    Hook.QUERY_PLAN.add((String s) -> {
      log.info("sql: " + s);
    });
  }

  /**
   * 执行sql语句，返回查询结果的个数
   *
   * @param sql sql查询语句
   * @return 查询结果的个数
   * @throws SQLException sql语句错误
   */
  protected int executeQuery(String sql) throws SQLException {
    Statement preparedStatement = calciteConnection.createStatement();
    preparedStatement.execute(sql);
    ResultSet resultSet = preparedStatement.getResultSet();
    int n = 0;
    ResultSetMetaData metaData = resultSet.getMetaData();
    while (resultSet.next()) {
      int columnCount = metaData.getColumnCount();
      for (int i = 0; i < columnCount; i++) {
        int idx = i + 1;
        String label = metaData.getColumnLabel(idx);
        Object object = resultSet.getObject(idx);
        log.info(label + ": " + object);
      }
      n++;
    }
    return n;
  }
}
