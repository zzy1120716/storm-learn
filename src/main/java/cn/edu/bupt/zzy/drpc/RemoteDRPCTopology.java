package cn.edu.bupt.zzy.drpc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @description: 远程DRPC
 * @author: zzy
 * @date: 2019-05-15 15:02
 **/
public class RemoteDRPCTopology {

    public static class MyBolt extends BaseRichBolt {

        private OutputCollector outputCollector;

        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.outputCollector = collector;
        }

        public void execute(Tuple input) {

            Object requestId = input.getValue(0);   // 请求的id
            String name = input.getString(1);   // 请求的参数

            /**
             * TODO... 业务逻辑处理
             */
            String result = "add user: " + name;

            this.outputCollector.emit(new Values(requestId, result));
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "result"));
        }
    }

    public static void main(String[] args) {
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("addUser");
        builder.addBolt(new MyBolt());

        try {
            StormSubmitter.submitTopology("drpc-topology", new Config(), builder.createRemoteTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
