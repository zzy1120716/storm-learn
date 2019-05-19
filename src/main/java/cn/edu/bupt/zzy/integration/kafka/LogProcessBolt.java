package cn.edu.bupt.zzy.integration.kafka;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @description: 接收Kafka的数据数据进行处理的Bolt
 * @author: zzy
 * @date: 2019/5/18
 **/
public class LogProcessBolt extends BaseRichBolt {

    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple input) {

        try {
            byte[] binaryByField = input.getBinaryByField("bytes");
            String value = new String(binaryByField);

            /**
             * 13788888888	116.544079,40.417555	[2019-05-19 18:43:23]
             *
             * 解析出来日志信息
             */

            String[] splits = value.split("\t");
            String phone = splits[0];
            String[] temp = splits[1].split(",");
            String longitude = temp[0];
            String latitude = temp[1];
            long time = DateUtils.getInstance().getTime(splits[2]);

            System.out.println(phone + ", " + longitude + ", " + latitude + ", " + time);

            collector.emit(new Values(time, Double.parseDouble(longitude), Double.parseDouble(latitude)));

            this.collector.ack(input);

        } catch (Exception e) {
            this.collector.fail(input);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("time", "longitude", "latitude"));
    }
}
