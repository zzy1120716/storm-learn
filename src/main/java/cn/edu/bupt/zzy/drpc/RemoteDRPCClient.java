package cn.edu.bupt.zzy.drpc;

import org.apache.storm.Config;
import org.apache.storm.utils.DRPCClient;

/**
 * @description: Remote DRPC客户端测试类
 * @author: zzy
 * @date: 2019-05-15 15:42
 **/
public class RemoteDRPCClient {

    public static void main(String[] args) throws Exception {

        Config config = new Config();

        // 按官网的代码跑不通，需要一组配置信息
        config.put("storm.thrift.transport", "org.apache.storm.security.auth.SimpleTransportPlugin");
        config.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        config.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
        config.put(Config.DRPC_MAX_BUFFER_SIZE, 1048576); // 1M

        DRPCClient client = new DRPCClient(config, "hadoop000", 3772);
        String result = client.execute("addUser", "lisi");

        System.out.println("Client invoked: " + result);

    }
}
