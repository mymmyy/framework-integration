package com.mym.repo;

import com.mym.entity.Person;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository("personRepo")
public interface PersonRepo extends MongoRepository<Person, String>,PersonRepoTemplate{
}
