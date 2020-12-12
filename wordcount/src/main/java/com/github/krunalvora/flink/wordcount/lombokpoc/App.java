package com.github.krunalvora.flink.wordcount.lombokpoc;

public class App {

  public static void main(String[] args) {
    Person person = new Person();
    person.setFirstname("Aijia");
    person.setLastname("Liu");
    System.out.println(person);
  }
}