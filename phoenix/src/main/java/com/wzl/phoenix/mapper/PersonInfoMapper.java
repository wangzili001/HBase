package com.wzl.phoenix.mapper;

import com.wzl.phoenix.entity.Person;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface PersonInfoMapper {

    @Insert("upsert into PERSON(ID,NAME,AGE,SEX) VALUES(#{person.id},#{person.name},#{person.age},#{person.sex})")
    public void addPerson(@Param("person") Person person);

    public Person getPerson(int id);

    public Person getPersonByName(String name);

    public List<Person> getAllPerson();
}
