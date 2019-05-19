package cn.edu.bupt.zzy.integration.kafka;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;
import java.util.UUID;

/**
 * @description: Kafka整合Storm测试
 * @author: zzy
 * @date: 2019/5/18
 **/
public class StormKafkaTopology {

    public static void main(String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        // Kafka使用的zk地址
        BrokerHosts hosts = new ZkHosts("hadoop000:2181");

        // Kafka存储数据的topic名称
        String topic = "project_topic";

        // 指定ZK中的一个根目录，存储的是KafkaSpout读取数据的位置信息（offset）
        String zkRoot = "/" + topic;
        String id = UUID.randomUUID().toString();

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);

        // 设置读取偏移量的操作
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

        String SPOUT_ID = KafkaSpout.class.getCanonicalName();
        builder.setSpout(SPOUT_ID, kafkaSpout);

        String BOLT_ID = LogProcessBolt.class.getSimpleName();
        builder.setBolt(BOLT_ID, new LogProcessBolt()).shuffleGrouping(SPOUT_ID);


        // JdbcInsertBolt的相关配置
        Map<String, Object> hikariConfigMap = Maps.newHashMap();
//        hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        hikariConfigMap.put("dataSourceClassName", "com.mysql.cj.jdbc.MysqlDataSource");
        hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/storm?useUnicode=true&useSSL=false&characterEncoding=UTF-8&serverTimezone=UTC");
        hikariConfigMap.put("dataSource.user", "root");
        hikariConfigMap.put("dataSource.password", "root");
        ConnectionProvider connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

        String tableName = "stat";
        JdbcMapper simpleJdbcMapper = new SimpleJdbcMapper(tableName, connectionProvider);

        JdbcInsertBolt userPersistanceBolt = new JdbcInsertBolt(connectionProvider, simpleJdbcMapper)
                .withTableName(tableName)
                .withQueryTimeoutSecs(30);

        builder.setBolt("JdbcInsertBolt", userPersistanceBolt).shuffleGrouping(BOLT_ID);



        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(StormKafkaTopology.class.getSimpleName(), new Config(), builder.createTopology());

    }
}
