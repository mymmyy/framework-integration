package com.mym.test;

import com.mym.entity.User;
import com.mym.service.UserService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:spring-context.xml")
public class TestUser {

    @Autowired
    private UserService userService;

    @Test
    public void testInsert(){
        userService.insert();
    }

    @Test
    public void testFind(){
        List<User> lists = userService.getLists();
        for(User user:lists){
            System.out.println(user);
        }

    }

    @Test
    public void testUpdate(){
        User user = new User();
        user.setId(3);
        user.setName("ym");
        user.setAddress("china");

        userService.updata(user);
    }

    @Test
    public void testDel(){
        User user = new User();
        user.setId(4);

        userService.delete(user);
    }

}
