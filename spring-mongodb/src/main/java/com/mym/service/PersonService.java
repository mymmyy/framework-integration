package com.mym.service;

import com.mym.entity.Person;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface PersonService {
    //添加数据
    void insertDB();

    //根据条件查询所有
    List<Person> findAll();

    //根据条件查询
    Person findOne();

    //修改
    void update();

    //删除
    void delete();

    //分页
    List<Person> getPersonPage();

}
