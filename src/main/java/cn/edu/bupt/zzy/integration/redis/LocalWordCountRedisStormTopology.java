package cn.edu.bupt.zzy.integration.redis;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.redis.bolt.RedisStoreBolt;
import org.apache.storm.redis.common.config.JedisPoolConfig;
import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.ITuple;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * @description: Storm整合Redis
 * @author: zzy
 * @date: 2019-05-09 20:30
 **/
public class LocalWordCountRedisStormTopology {

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
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static class WordCountStoreMapper implements RedisStoreMapper {

        private RedisDataTypeDescription description;
        private final String hashKey = "wc";

        public WordCountStoreMapper() {
            description = new RedisDataTypeDescription(
                    RedisDataTypeDescription.RedisDataType.HASH, hashKey);
        }

        public RedisDataTypeDescription getDataTypeDescription() {
            return description;
        }

        public String getKeyFromTuple(ITuple tuple) {
            return tuple.getStringByField("word");
        }

        public String getValueFromTuple(ITuple tuple) {
            return tuple.getIntegerByField("count") + "";
        }
    }

    public static void main(String[] args) {

        // 通过TopologyBuilder根据Spout和Bolt构建Topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());
        builder.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("DataSourceSpout");
        builder.setBolt("CountBolt", new CountBolt()).shuffleGrouping("SplitBolt");

        // 配置topology中的RedisStoreBolt
        JedisPoolConfig poolConfig = new JedisPoolConfig.Builder()
                .setHost("hadoop000").setPort(6379).build();
        RedisStoreMapper storeMapper = new WordCountStoreMapper();
        RedisStoreBolt storeBolt = new RedisStoreBolt(poolConfig, storeMapper);
        builder.setBolt("RedisStoreBolt", storeBolt).shuffleGrouping("CountBolt");

        // 创建本地集群
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LocalWordCountRedisStormTopology", new Config(), builder.createTopology());

    }

}
