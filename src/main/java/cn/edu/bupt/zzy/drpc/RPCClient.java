package cn.edu.bupt.zzy.drpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

/**
 * @description: RPC客户端
 * @author: zzy
 * @date: 2019-05-15 14:22
 **/
public class RPCClient {

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();

        long clientVersion = 88888888;

        // 拿到远程的一个服务
        UserService userService = RPC.getProxy(UserService.class, clientVersion, new InetSocketAddress("localhost", 9999), configuration);

        userService.addUser("lisi", 30);

        System.out.println("From Client... invoked");

        RPC.stopProxy(userService);

    }
}
