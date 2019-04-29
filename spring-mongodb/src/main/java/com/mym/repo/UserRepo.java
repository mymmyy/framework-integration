package com.mym.repo;

import org.springframework.data.mongodb.repository.MongoRepository;

import com.mym.entity.User;
import org.springframework.stereotype.Repository;

@Repository
public interface UserRepo extends MongoRepository<User, String>{

}
