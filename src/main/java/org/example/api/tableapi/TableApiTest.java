package org.example.api.tableapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.example.SensorReading;

/**
 * @author machenggong
 * @since 2021/5/30
 */
public class TableApiTest {

    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String inputPath = "/Users/machenggong/project/github/flink-quickstart/src/main/resources/sensor.txt";
        // DataSet
        DataStreamSource<String> inputStream = env.readTextFile(inputPath);
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table dataTable = tableEnv.fromDataStream(dataStream);

        //调用tableApi 进行转换操作

        Table resultTable = dataTable.select("id, temperature").where("id = 'sensor_1'");
        // 类似于视图功能
        tableEnv.registerTable("sensor",dataTable);
        //tableEnv.c("sensor", dataTable);
        //dataTable.createTemporalTableFunction()''
        String sql = "select id,temperature from sensor where id = 'sensor_1'";
        Table resultSqlTable = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");
        env.execute();
    }

}
