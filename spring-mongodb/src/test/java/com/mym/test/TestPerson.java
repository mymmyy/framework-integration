package com.mym.test;

import com.mym.entity.Person;
import com.mym.service.PersonService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:spring-context.xml")
public class TestPerson {

    @Autowired
    PersonService personService;

    @Test
    public void testInsert(){
        personService.insertDB();
    }

    @Test
    public void testFindAll(){
        List<Person> all = personService.findAll();
        for(Person p:all){
            System.out.println(p);
        }
    }

    @Test
    public void testFindOne(){
        Person p = personService.findOne();
        System.out.println(p);
    }

    @Test
    public void testUpdate(){
        personService.update();
    }

    @Test
    public void testDelete(){
        personService.delete();
    }

    @Test
    public void testGetPersonPages(){
        List<Person> personPage = personService.getPersonPage();
        System.out.println("count is："+personPage.size());
        for(Person p:personPage){
            System.out.println(p);
        }
    }

}
