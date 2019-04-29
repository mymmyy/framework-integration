package com.mym.service;

import com.mym.entity.User;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface UserService {

    List<User> getLists();

    void updata(User user);

    void insert();

    void delete(User user);
}
