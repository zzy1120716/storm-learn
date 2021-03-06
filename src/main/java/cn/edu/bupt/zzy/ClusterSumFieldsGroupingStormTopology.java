package cn.edu.bupt.zzy;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
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

import java.util.Map;

/**
 * @description: Fields Grouping使用
 * @author: zzy
 * @date: 2019-05-09 19:38
 **/
public class ClusterSumFieldsGroupingStormTopology {

    /**
     * Spout需要继承BaseRichSpout
     * 数据源需要产生数据并发射
     */
    public static class DataSourceSpout extends BaseRichSpout {

        private SpoutOutputCollector collector;

        /**
         * 初始化方法，只会被调用一次
         * @param conf 配置参数
         * @param context 上下文
         * @param collector 数据发射器
         */
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;
        }

        int number = 0;

        /**
         * 会产生数据，在生产上肯定是从消息队列中获取数据
         *
         * 这个方法是一个死循环，会一直不停的执行
         */
        public void nextTuple() {
            // 这里发射的数据第一个字段是用来区分奇偶的
            this.collector.emit(new Values(number % 2, ++number));

            System.out.println("Spout: " + number);

            // 防止数据产生太快
            Utils.sleep(1000);
        }

        /**
         * 声明输出字段
         * @param declarer
         */
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            // 此处增加第一个字段的声明
            declarer.declare(new Fields("flag", "num"));
        }

    }


    /**
     * 数据的累积求和Bolt：接收数据并处理
     */
    public static class SumBolt extends BaseRichBolt {

        /**
         * 初始化方法，会被执行一次
         * @param stormConf
         * @param context
         * @param collector
         */
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        }

        int sum = 0;

        /**
         * 其实也是一个死循环，职责：获取Spout发送过来的数据
         * @param input
         */
        public void execute(Tuple input) {
            // Bolt中获取值可以根据index获取，也可以根据上一个环节中定义的field的名称获取（建议使用该方式）
            // 此处通过字段名来获取数据，就是上面Spout中发射的字段名
            Integer value = input.getIntegerByField("num");
            sum += value;

            System.out.println("Bolt: sum = [" + sum + "]");
            System.out.println("Thread id: " + Thread.currentThread().getId() + " , received data is: " + value);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }


    public static void main(String[] args) {

        // TopologyBuilder根据Spout和Bolt来构建出Topology
        // Storm中任何一个作业都是通过Topology的方式进行提交的
        // Topology中需要指定Spout和Bolt的执行顺序
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("DataSourceSpout", new DataSourceSpout());

        // 由于flag字段只有两个值，所以并行度是3时，只有两个线程在干活
        //builder.setBolt("SumBolt", new SumBolt(), 3).fieldsGrouping("DataSourceSpout", new Fields("flag"));

        // 问题：若只有一个线程，flag有两个值，会如何被处理，是丢弃，还是一个线程处理所有数据？
        // 据观察，是一个线程处理Spout的所有tuple
        builder.setBolt("SumBolt", new SumBolt(), 1).fieldsGrouping("DataSourceSpout", new Fields("flag"));


        // 代码提交到Storm集群上运行
        String topoName = ClusterSumFieldsGroupingStormTopology.class.getSimpleName();
        try {
            StormSubmitter.submitTopology(topoName, new Config(), builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
