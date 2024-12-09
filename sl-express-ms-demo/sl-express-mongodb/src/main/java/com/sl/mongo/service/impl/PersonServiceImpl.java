package com.sl.mongo.service.impl;

import com.sl.mongo.entity.Person;
import com.sl.mongo.service.PersonService;
import org.bson.types.ObjectId;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

@Service
public class PersonServiceImpl implements PersonService {

    @Resource
    private MongoTemplate mongoTemplate;

    @Override
    public void savePerson(Person person) {
        this.mongoTemplate.save(person);
    }

    @Override
    public void update(Person person) {
        //条件
        Query query = Query.query(Criteria.where("id").is(person.getId()));

        //更新的数据
        Update update = Update.update("age", person.getAge())
                .set("name", person.getName())
                .set("location", person.getLocation())
                .set("address", person.getAddress());

        //更新数据
        this.mongoTemplate.updateFirst(query, update, Person.class);
    }

    @Override
    public List<Person> queryPersonListByName(String name) {
        Query query = Query.query(Criteria.where("name").is(name)); //构造查询条件
        return this.mongoTemplate.find(query, Person.class);
    }

    @Override
    public List<Person> queryPersonPageList(int page, int pageSize) {
        PageRequest pageRequest = PageRequest.of(page - 1, pageSize);
        Query query = new Query().with(pageRequest);
        return this.mongoTemplate.find(query, Person.class);
    }

    @Override
    public void deleteById(String id) {
        Query query = Query.query(Criteria.where("id").is(new ObjectId(id)));
        this.mongoTemplate.remove(query, Person.class);
    }
}
