//package org.fasteam.streaming.entry;
//
//import com.alibaba.fastjson.JSON;
//import com.tm.dl.javasdk.dpspark.DPSparkApp;
//import com.tm.dl.javasdk.dpspark.common.DataTools;
//import com.tm.dl.javasdk.dpspark.common.entity.*;
//import com.tm.dl.javasdk.dpspark.common.streaming.StructuredFunction2;
//import com.tm.dl.javasdk.dpspark.common.streaming.StructuredWatermarkFunction;
//import com.tm.dl.javasdk.dpspark.hbase.DPShc;
//import com.treasuremountain.tmcommon.thirdpartyservice.redis.TMRedisOperator;
//import com.treasuremountain.tmcommon.thirdpartyservice.websocket.WebsocketClient;
//import com.treasuremountain.tmcommon.thirdpartyservice.websocket.data.MessageStreaming;
//import lombok.SneakyThrows;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.client.Table;
//import org.apache.hadoop.hbase.spark.JavaHBaseContext;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.function.VoidFunction;
//import org.apache.spark.api.java.function.VoidFunction2;
//import org.apache.spark.sql.*;
//import org.apache.spark.sql.streaming.DataStreamReader;
//import org.apache.spark.sql.streaming.DataStreamWriter;
//import org.apache.spark.sql.types.StructType;
//import scala.Function2;
//import scala.Tuple2;
//
//import java.io.IOException;
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.function.Consumer;
//import java.util.function.Function;
//import java.util.function.Supplier;
//
//
///**
// * Description:  structured streaming开发
// * Copyright: © 2021 Foxconn. All rights reserved.
// * Company: Foxconn
// *
// * @author FL
// * @version 1.0
// * @timestamp 2021/5/7
// */
//public class StructuredStreaming {
//    private static final String WEBSOCKET_URL = "ws://dpwebsocket:9100/webSocketServer";
//    private static final String MESSAGE_URL = "/message/dashboard/onmessage";
//    private static final String MYSQL_DB = "mysql";
//
//    public enum OutputModeEnum {
//        APPEND,
//        COMPLETE,
//        UPDATE,
//        NONE;
//    }
//
//    private List<StreamFrame> streamFrameList;
//    private KafkaStruct kafkaInfo;
//    private List<OutputKafkaFrame> outputKafkaFrames;
//    private List<OutputWsFrame> outputWsFrames;
//    private List<OutputRdbFrame> outputRdbFrames;
//    private List<OutputHbaseFrame> outputHbaseFrames;
//
//
//    public DPStructuredStream subscribe(Consumer<TopicInfo> schemaFunction, Consumer<List<StreamFrame>> function) throws Exception {
//        checkMethodCallOnce();
//        kafkaInfo = DPSparkApp.getKafkaStruct();
//        List<TopicInfo> topicList = kafkaInfo.getSourceTopics();
//        streamFrameList = new ArrayList<>();
//        //如果有多个topicInfo的时候，需要分别去连接，作为不同的数据源，读取出来也是不同的流
//        //如果是单个topicInfo中的多个topic可以通过逗号拼接，会把这些topic集成到一个流中
//        for (TopicInfo topicInfo : topicList) {
//            StreamFrame streamFrame = new StreamFrame();
//            schemaFunction.accept(topicInfo);
//            Dataset<Row> streamDataset = readStreamWithKerberos(topicInfo.getTopics());
//            streamFrame.setName(topicInfo.getName());
//            streamFrame.setStreamSet(flatMapFromJson(streamDataset, topicInfo.getSchema()));
//            streamFrameList.add(streamFrame);
//        }
//        function.accept(streamFrameList);
//        return this;
//    }
//
//    //读取一张静态的hbase表，用于后续的维度关联
//    public DPStructuredStream subscribeFromStatic(Supplier<List<ImpressionsStatic>> schemaFunction, Consumer<List<StreamFrame>> function) throws Exception {
//        if (streamFrameList == null) {
//            streamFrameList = new ArrayList<>();
//        }
//        List<ImpressionsStatic> statics = schemaFunction.get();
//        for (ImpressionsStatic impressionsStatic : statics) {
//            StreamFrame streamFrame = new StreamFrame();
//            Dataset<Row> staticDataset = DPShc.sqlRead(impressionsStatic.getTableName());
//            if (staticDataset != null) {
//                staticDataset.cache();
//            }
//            streamFrame.setStreamSet(staticDataset);
//            streamFrame.setName(impressionsStatic.getName());
//            streamFrameList.add(streamFrame);
//        }
//        function.accept(streamFrameList);
//        return this;
//    }
//
//    //需要指定事件时间列，一定要先指定事件列和水位线才能开始执行ds相关操作
//    public DPStructuredStream subscribeWithWatermark(Consumer<TopicInfo> schemaFunction, StructuredWatermarkFunction windowFunction, Consumer<List<StreamFrame>> function) throws Exception {
//        checkMethodCallOnce();
//        kafkaInfo = DPSparkApp.getKafkaStruct();
//        List<TopicInfo> topicList = kafkaInfo.getSourceTopics();
//        streamFrameList = new ArrayList<>();
//        for (TopicInfo topicInfo : topicList) {
//            StreamFrame streamFrame = new StreamFrame();
//            schemaFunction.accept(topicInfo);
//            Dataset<Row> streamDataset = readStreamWithKerberos(topicInfo.getTopics());
//            streamFrame.setName(topicInfo.getName());
//            streamFrame.setStreamSet(windowFunction.accept(topicInfo.getName(), flatMapFromJson(streamDataset, topicInfo.getSchema())));
//            streamFrameList.add(streamFrame);
//        }
//        function.accept(streamFrameList);
//        return this;
//    }
//
//    public DPStructuredStream setupOutputKafkaFrame(StructuredFunction2<List<TopicInfo>, List<StreamFrame>, List<OutputKafkaFrame>> flapMapOutputFrame) {
//        List<OutputKafkaFrame> outputFrames = flapMapOutputFrame.apply(this.kafkaInfo.getSinkTopics(), this.streamFrameList);
//        this.outputKafkaFrames = outputFrames;
//        return this;
//    }
//
//
//    public void writeKafkaSink(Function<String, OutputModeEnum> outputModeFunction) throws Exception {
//        checkMethodCallFirst();
//        checkMethodCallPre();
//        for (OutputKafkaFrame frame : this.outputKafkaFrames) {
//            //要根据source的name去匹配sink的name，这样才能把接收至发送串起来
//            TopicInfo outputTopic = frame.getTopicInfo();
//            StreamFrame streamFrame = frame.getStreamFrame();
//            OutputModeEnum outputMode = outputModeFunction.apply(outputTopic.getName());
//            switch (outputMode) {
//                case NONE:
//                    streamFrame.getStreamSet().writeStream()
//                            .format("kafka")
//                            .option("kafka.bootstrap.servers", this.kafkaInfo.getSinkServerUrl())
//                            .option("topic", outputTopic.getTopics())
//                            .start();
//                    break;
//                case COMPLETE:
//                    streamFrame.getStreamSet().writeStream()
//                            .format("kafka")
//                            .option("kafka.bootstrap.servers", this.kafkaInfo.getSinkServerUrl())
//                            .option("topic", outputTopic.getTopics())
//                            .outputMode("complete")
//                            .start();
//                    break;
//                case APPEND:
//                    streamFrame.getStreamSet().writeStream()
//                            .format("kafka")
//                            .option("kafka.bootstrap.servers", this.kafkaInfo.getSinkServerUrl())
//                            .option("topic", outputTopic.getTopics())
//                            .outputMode("append")
//                            .start();
//                    break;
//                case UPDATE:
//                    streamFrame.getStreamSet().writeStream()
//                            .format("kafka")
//                            .option("kafka.bootstrap.servers", this.kafkaInfo.getSinkServerUrl())
//                            .option("topic", outputTopic.getTopics())
//                            .outputMode("update")
//                            .start();
//                    break;
//            }
//        }
//
//
//    }
//
//    /**
//     * 每个流式dataFrame都需要能有个对应的ws的socketId，这样才能知道这条流要发送到哪里去
//     *
//     * @param flapMapOutputFrame
//     */
//    public DPStructuredStream setupOutputWsFrame(Function<List<StreamFrame>, List<OutputWsFrame>> flapMapOutputFrame) {
//        List<OutputWsFrame> outputFrames = flapMapOutputFrame.apply(this.streamFrameList);
//        this.outputWsFrames = outputFrames;
//        return this;
//    }
//
//    public void writeWebsocketSink(Function<Row, String> rowTranslator, Map headers) throws Exception {
//        checkMethodCallFirst();
//        checkMethodCallPre();
//        for (OutputWsFrame frame : this.outputWsFrames) {
//            StreamFrame outputFrame = frame.getStreamFrame();
//            String sockId = frame.getSockId();
//            outputFrame.getStreamSet().writeStream().foreach(new ForeachWriter<Row>() {
//                private WebsocketClient websocketClient;
//
//                @Override
//                public void process(Row value) {
//                    String jsonContent = rowTranslator.apply(value);
//                    MessageStreaming messageStreaming = new MessageStreaming();
//                    messageStreaming.setContent(jsonContent);
//                    messageStreaming.setSock(sockId);
//                    messageStreaming.setFlow(true);
//                    messageStreaming.setEndTag(false);
//                    String message = JSON.toJSONString(messageStreaming);
//                    websocketClient.send(MESSAGE_URL, message);
//                }
//
//                @Override
//                public void close(Throwable errorOrNull) {
//                    //执行process时产生的运行时异常会可以在这个方法参数中得到
//                    if (errorOrNull != null) {
//                        log.error("发送websocket消息发生异常", errorOrNull);
//                    }
//                    websocketClient.disconnection();
//                }
//
//                @Override
//                public boolean open(long partitionId, long epochId) {
//                    //按分区的方式进行连接，每个分区都会单独处理
//                    websocketClient = new WebsocketClient();
//                    websocketClient.connect(WEBSOCKET_URL, headers);
//                    //TODO 连接发生异常会有数据丢失风险
//                    return true;
//                }
//            }).start();
//        }
//    }
//
//    /**
//     * 每个流式dataFrame都需要能有个对应的ws的socketId，这样才能知道这条流要发送到哪里去
//     *
//     * @param flapMapOutputFrame
//     */
//    public DPStructuredStream setupOutputRdbFrame(Function<List<StreamFrame>, List<OutputRdbFrame>> flapMapOutputFrame) {
//        List<OutputRdbFrame> outputFrames = flapMapOutputFrame.apply(this.streamFrameList);
//        this.outputRdbFrames = outputFrames;
//        return this;
//    }
//
//    public void writeMysqlSink(StructuredFunction2<String, Row, String> sqlTranslator) throws Exception {
//        checkMethodCallFirst();
//        checkMethodCallPre();
//        for (OutputRdbFrame outputRdbFrame : this.outputRdbFrames) {
//            String dbCode = outputRdbFrame.getDbCode();
//            StreamFrame streamFrame = outputRdbFrame.getStreamFrame();
//            streamFrame.getStreamSet().writeStream().foreach(new ForeachWriter<Row>() {
//                private Connection connection;
//                private Statement statement;
//
//                @SneakyThrows
//                @Override
//                public boolean open(long partitionId, long epochId) {
//                    RDBConnetInfo connectionInfo = DPSparkApp.getRDBConnectionInfo(MYSQL_DB, dbCode);
//                    Class.forName("com.mysql.jdbc.Driver");
//                    connection = DriverManager.getConnection(connectionInfo.getDbUrl(), connectionInfo.getDbUsername(), connectionInfo.getDbPassword());
//                    statement = connection.createStatement();
//                    return true;
//                }
//
//                @SneakyThrows
//                @Override
//                public void process(Row value) {
//                    String sql = sqlTranslator.apply(streamFrame.getName(), value);
//                    statement.executeUpdate(sql);
//                }
//
//                @SneakyThrows
//                @Override
//                public void close(Throwable errorOrNull) {
//                    connection.close();
//                }
//            }).start();
//        }
//    }
//
//    /**
//     * 每个流式dataFrame都需要能有个对应的ws的socketId，这样才能知道这条流要发送到哪里去
//     *
//     * @param flapMapOutputFrame
//     */
//    public DPStructuredStream setupOutputHbaseFrame(Function<List<StreamFrame>, List<OutputHbaseFrame>> flapMapOutputFrame) {
//        List<OutputHbaseFrame> outputFrames = flapMapOutputFrame.apply(this.streamFrameList);
//        this.outputHbaseFrames = outputFrames;
//        return this;
//    }
//
//    public void writeHbaseSink(Function<Dataset<Row>, JavaRDD<Put>> dataTranslator) throws Exception {
//        checkMethodCallFirst();
//        checkMethodCallPre();
//        Configuration configuration = DPSparkApp.getDpPermissionManager().initialHbaseSecurityContext();
//        JavaHBaseContext hBaseContext = new JavaHBaseContext(DPSparkApp.getContext(), configuration);
//        for (OutputHbaseFrame outputHbaseFrame : this.outputHbaseFrames) {
//            String tableName = outputHbaseFrame.getTableName();
//            StreamFrame streamFrame = outputHbaseFrame.getStreamFrame();
//            HBTableEntity hbTableEntity = DPSparkApp.getEntityByTableName(tableName);
//            streamFrame.getStreamSet().writeStream().foreachBatch(new VoidFunction2<Dataset<Row>, Long>() {
//                @Override
//                public void call(Dataset<Row> dataset, Long batchId) throws Exception {
//                    dataset.persist();
//                    //dataset转put
//                    JavaRDD<Put> puts = dataTranslator.apply(dataset);
//
//                    hBaseContext.foreachPartition(puts, new VoidFunction<Tuple2<Iterator<Put>, org.apache.hadoop.hbase.client.Connection>>() {
//                        @Override
//                        public void call(Tuple2<Iterator<Put>, org.apache.hadoop.hbase.client.Connection> tuple2) throws Exception {
//                            try {
//                                Table table = tuple2._2().getTable(TableName.valueOf(hbTableEntity.getHbcurrenttablename()));
//                                Iterator<Put> putIterator = tuple2._1();
//                                List<Put> puts = new ArrayList<>();
//                                putIterator.forEachRemaining(p -> {
//                                    puts.add(p);
//                                });
//                                Object[] results = new Object[puts.size()];
//                                table.batch(puts, results);
//                                table.close();
//                            } catch (Exception e) {
//                                log.error("写入" + hbTableEntity.getHbcurrenttablename() + "失败,请检查hbase表状态是否异常");
//                            }
//                        }
//                    });
//                }
//            }).start();
//        }
//    }
//
//    private void checkMethodCallFirst() throws Exception {
//        if (this.streamFrameList == null || this.streamFrameList.isEmpty()) {
//            throw new Exception("please call subscribe method first.");
//        }
//    }
//
//    private void checkMethodCallOnce() throws Exception {
//        if (this.streamFrameList != null) {
//            throw new Exception("subscribe* method could only call once.");
//        }
//    }
//
//    private void checkMethodCallPre() throws Exception {
//        if (this.outputKafkaFrames == null || this.outputKafkaFrames.isEmpty()) {
//            throw new Exception("please call setup* method before write to any sink.");
//        }
//    }
//
//    private Dataset<Row> flatMapFromJson(Dataset<Row> jsonSet, StructType schema) {
//        Dataset<Row> ds1 = jsonSet.selectExpr("CAST(value AS STRING) as dl_json");
//        return ds1.select(functions.from_json(ds1.col("dl_json"), schema).as("dl_data")).select("dl_data.*");
//    }
//
//    private Dataset<Row> readStreamWithKerberos(String topics) {
//        SparkSession session = DPSparkApp.getSession();
//        return session.readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", kafkaInfo.getSourceServerUrl())
//                .option("subscribe", topics)
//                .option("kafka.security.protocol", "SASL_PLAINTEXT")
//                .option("kafka.sasl.mechanism", "GSSAPI")
//                .option("kafka.sasl.kerberos.service.name", "kafka")
//                .option("kafka.sasl.jaas.config", this.kerberosStruct.getJaasConfig())
////                    .schema(topicInfo.getSchema())
//                .load();
//    }
//
//}
