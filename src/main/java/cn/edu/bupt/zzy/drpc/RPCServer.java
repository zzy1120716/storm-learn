package cn.edu.bupt.zzy.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * @description: RPC Server服务
 * @author: zzy
 * @date: 2019-05-15 14:17
 **/
public class RPCServer {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        RPC.Builder builder = new RPC.Builder(configuration);

        // Java Builder模式
        RPC.Server server = builder.setProtocol(UserService.class)
                .setInstance(new UserServiceImpl())
                .setBindAddress("localhost")
                .setPort(9999)
                .build();

        server.start();

    }
}
