package com.mym.repo;

import com.mym.entity.Person;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public interface PersonRepoTemplate {

    //添加数据
    void insertDB(Person person);

    //根据条件查询所有
    List<Person> findAll(Map<String, Object> params);

    //根据条件查询
    Person findOne(Map<String, Object> params);

    //修改
    void update(Map<String, Object> params);

    //删除
    void delete(Map<String, Object> params);

    //分页查询
    List<Person> getPersonPage(int pageNo, int pageSize);

}
