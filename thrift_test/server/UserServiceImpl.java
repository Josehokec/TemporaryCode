package thrift_test.server;

import org.apache.thrift.TException;
import thrift_test.User;
import thrift_test.UserService;

import java.util.ArrayList;
import java.util.List;

public class UserServiceImpl implements UserService.Iface {
    private List<User> users = new ArrayList<>();

    @Override
    public User getByOd(int id) throws TException {
        // 存储端模拟服务调用函数
        User user = new User(id, String.valueOf(id), 18);
        users.add(user);
        return user;
    }

    @Override
    public boolean isExist(String name) throws TException {
        for (User user : users) {
            if (user.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
