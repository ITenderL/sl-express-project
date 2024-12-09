package com.sl.mongo;

import com.sl.mongo.entity.Address;
import com.sl.mongo.entity.Person;
import com.sl.mongo.service.PersonService;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;

import javax.annotation.Resource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class PersonServiceTest {

    @Resource
    PersonService personService;

    @Test
    void savePerson() {
        Person person = Person.builder()
                .id(ObjectId.get())
                .name("张三")
                .age(20)
                .location(new GeoJsonPoint(116.343847, 40.060539))
                .address(new Address("人民路", "上海市", "666666")).build();
        this.personService.savePerson(person);
    }

    @Test
    void update() {
        Person person = Person.builder()
                .id(new ObjectId("632283c08139e535c2bd7579"))
                .name("张三")
                .age(22) //修改数据
                .location(new GeoJsonPoint(116.343847, 40.060539))
                .address(new Address("人民路", "上海市", "666666")).build();
        this.personService.update(person);
    }

    @Test
    void queryPersonListByName() {
        List<Person> personList = this.personService.queryPersonListByName("张三");
        personList.forEach(System.out::println);
    }

    @Test
    void queryPersonPageList() {
        List<Person> personList = this.personService.queryPersonPageList(1, 10);
        personList.forEach(System.out::println);
    }

    @Test
    void deleteById() {
        this.personService.deleteById("632283c08139e535c2bd7579");
    }
}