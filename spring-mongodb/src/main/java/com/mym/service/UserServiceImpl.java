package com.mym.service;

import com.mym.entity.User;
import com.mym.repo.UserRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserServiceImpl implements UserService{

    @Autowired
    private UserRepo userRepo;

    @Override
    public List<User> getLists() {
        return userRepo.findAll();
    }

    @Override
    public void updata(User user) {
        userRepo.save(user);
    }

    @Override
    public void insert() {
        User user = new User();
        for(int i = 0; i < 10 ;i++){
            user.setId(i);
            user.setAddress("省"+i);
            user.setName("name"+i);

            User insert = userRepo.insert(user);
            System.out.println(insert.getId()+", "+insert);
        }
    }

    @Override
    public void delete(User user) {

        userRepo.delete(user);
    }
}
