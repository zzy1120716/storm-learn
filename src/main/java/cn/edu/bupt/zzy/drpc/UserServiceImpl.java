package cn.edu.bupt.zzy.drpc;

/**
 * @description: 用户服务接口实现类
 * @author: zzy
 * @date: 2019-05-15 14:16
 **/
public class UserServiceImpl implements UserService {

    public void addUser(String name, int age) {
        System.out.println("From Server Invoked: add user success..., name is: " + name);
    }
}
