package cn.edu.bupt.zzy.integration.jdbc;

import com.google.common.collect.Maps;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.sql.Types;
import java.util.*;

/**
 * @description: Storm整合JDBC改进，先查记录是否已存在，若存在，则update，否则，insert。
 * @author: zzy
 * @date: 2019-05-09 20:30
 **/
public class LocalWordCountJDBCUpdateStormTopology {

    /**
     * 第一步：读取文件，产生数据是一行行的文本
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        public static final String[] words = new String[]{"apple", "orange", "pineapple", "banana", "watermelon"};

        public void nextTuple() {

            Random random = new Random();
            String word = words[random.nextInt(words.length)];

            this.collector.emit(new Values(word));

            System.out.println("emit: " + word);

            Utils.sleep(1000);

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line"));
        }
    }

    /**
     * 第二步：分割文本，产生一个个单词
     */
    public static class SplitBolt extends BaseRichBolt {

        private OutputCollector collector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        /**
         * 业务逻辑：
         */
        public void execute(Tuple input) {

            String word = input.getStringByField("line");
            this.collector.emit(new Values(word));

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    /**
     * 第三步：词频汇总Bolt
     */
    public static class CountBolt extends BaseRichBolt {

        private OutputCollector collector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector = collector;
        }

        Map<String, Integer> map = new HashMap<String, Integer>();

        /**
         * 业务逻辑：
         * 1）获取每个单词
         * 2）对所有单词进行汇总
         * 3）输出
         */
        public void execute(Tuple input) {
            // 1）获取每个单词
            String word = input.getStringByField("word");
            Integer count = map.get(word);
            if (count == null) {
                count = 0;
            }

            count++;

            // 2）对所有单词进行汇总
            map.put(word, count);

            // 3）输出
            this.collector.emit(new Values(word, map.get(word)));

        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "word_count"));
        }
    }


    /**
     * 第四步：查询MySQL中是否已有相应记录，若有，则update，否则，insert
     */
    public static class JdbcUpdateBolt extends BaseRichBolt {

        private OutputCollector collector;
        private JdbcClient jdbcClient;
        private ConnectionProvider connectionProvider;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

            this.collector = collector;

            // JdbcInsertBolt的相关配置
            Map<String, Object> hikariConfigMap = Maps.newHashMap();
//        hikariConfigMap.put("dataSourceClassName", "com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
            hikariConfigMap.put("dataSourceClassName", "com.mysql.cj.jdbc.MysqlDataSource");
            hikariConfigMap.put("dataSource.url", "jdbc:mysql://localhost/storm?useUnicode=true&useSSL=false&characterEncoding=UTF-8&serverTimezone=UTC");
            hikariConfigMap.put("dataSource.user", "root");
            hikariConfigMap.put("dataSource.password", "1q2w3e4r");
            connectionProvider = new HikariCPConnectionProvider(hikariConfigMap);

            // 对数据库连接池进行初始化
            connectionProvider.prepare();
            jdbcClient = new JdbcClient(connectionProvider, 30);
        }

        Map<String, Integer> map = new HashMap<String, Integer>();

        public void execute(Tuple input) {

            String word = input.getStringByField("word");
            Integer count = input.getIntegerByField("word_count");

            // 查询操作
            List<Column> list = new ArrayList<Column>();
            list.add(new Column("word", Types.VARCHAR));
            List<List<Column>> select = jdbcClient.select("select word from wc where word = ?", list);

            Long n = select.stream().count();
            System.out.println("Count of word '" + word + "' is: " + n);

            if (n >= 1) {
                // update
                jdbcClient.executeSql("update wc set word_count = " + count + " where word = '" + word + "'");
            } else {
                // insert
                jdbcClient.executeSql("insert into wc values ( '" + word + "', " + count + " )");
            }
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }

        @Override
        public void cleanup() {
            connectionProvider.cleanup();
        }
    }


    public static void main(String[] args) {

        // 通过TopologyBuilder根据Spout和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");

        builder.setBolt("JdbcUpdateBolt", new JdbcUpdateBolt()).shuffleGrouping("CountBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountJDBCUpdateStormTopology", new Config(), builder.createTopology());

    }

}
