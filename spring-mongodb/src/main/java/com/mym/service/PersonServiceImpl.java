package com.mym.service;


import com.mym.entity.Person;
import com.mym.repo.PersonRepo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;

@Service
public class PersonServiceImpl implements PersonService {

    @Autowired
    PersonRepo personRepo;

    @Override
    public void insertDB() {

        for(int i = 0;i < 5;i++){
            Person person = new Person();
            person.setId(i);
            person.setAddress("guangdong"+i);
            person.setName("m"+i);
            personRepo.insertDB(person);
        }
    }

    @Override
    public List<Person> findAll() {
        HashMap<String, Object> params = new HashMap<>();
        params.put("name","m1");
        return personRepo.findAll(params);
    }

    @Override
    public Person findOne() {
        HashMap<String, Object> params = new HashMap<>();
        params.put("name","m2");
        return personRepo.findOne(params);
    }

    @Override
    public void update() {
        HashMap<String, Object> params = new HashMap<>();
        params.put("name","m3");
        params.put("address","china");
        personRepo.update(params);
    }

    @Override
    public void delete() {
        HashMap<String, Object> params = new HashMap<>();
        params.put("name","m4");
        personRepo.delete(params);
    }

    @Override
    public List<Person> getPersonPage() {
        List<Person> personPage = personRepo.getPersonPage(2, 2);
        return personPage;
    }
}
