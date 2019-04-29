package com.mym.repo.impl;

import com.mym.entity.MongoDataPages;
import com.mym.entity.Person;
import com.mym.repo.PersonRepoTemplate;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Component
public class PersonRepoTemplateImpl implements PersonRepoTemplate {

    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public void insertDB(Person person) {

        mongoTemplate.insert(person);
    }

    @Override
    public List<Person> findAll(Map<String, Object> params) {
        Query query = new Query(Criteria.where("age").is(params.get("age")));
        return mongoTemplate.find(query, Person.class);
    }

    @Override
    public Person findOne(Map<String, Object> params) {
        Query query = new Query(Criteria.where("name").is(params.get("name")));
        return mongoTemplate.findOne(query,Person.class);
    }

    @Override
    public void update(Map<String, Object> params) {
        Query query = new Query(Criteria.where("name").is(params.get("name")));
        mongoTemplate.updateFirst(query, new Update().addToSet("age",params.get("age")),Person.class);
    }

    @Override
    public void delete(Map<String, Object> params) {
        Query query = new Query(Criteria.where("name").is(params.get("name")));
        mongoTemplate.remove(query);
    }

    @Override
    public List<Person> getPersonPage(int pageNo, int pageSize) {
        Page<Person> resultPages = null;
        try{
            Query query = new Query();
            MongoDataPages mongoDataPages = new MongoDataPages();
            mongoDataPages.setPageNo(pageNo);
            mongoDataPages.setPageSize(pageSize);

            long count = mongoTemplate.count(query, Person.class);
            List<Person> people = mongoTemplate.find(query.with(mongoDataPages), Person.class);
            resultPages = new PageImpl<Person>(people, mongoDataPages,count);
        }catch (Exception e){
            e.printStackTrace();
        }
        return resultPages.getContent();
    }
}
