package com.github.churtado.mockito;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

public class FirstTest {

    @DisplayName("mock a list")
    @Test
    void test1() {

        // mock creation
        List mockedList = mock(List.class);

        // use mock object
        mockedList.add("one");
        mockedList.clear();

        // verify
        verify(mockedList).add("one");
        verify(mockedList).clear();

    }

    @DisplayName("stubbing")
    @Test
    void test2() {

        // You can mock concrete classes, not just interfaces
        LinkedList mockedList = mock(LinkedList.class);

        // stubbing
        when(mockedList.get(0)).thenReturn("first");
        when(mockedList.get(1)).thenReturn(new RuntimeException());

        System.out.println(mockedList.get(0));
        System.out.println(mockedList.get(1));
        System.out.println(mockedList.get(999));

        //stubbing using built-in anyInt() argument matcher
        when(mockedList.get(anyInt())).thenReturn("element");

        //following prints "element"
        System.out.println(mockedList.get(999));

        verify(mockedList).get(0);

    }

    @DisplayName("some more stuff")
    @Test
    void test3() {

        // You can mock concrete classes, not just interfaces
        LinkedList mockedList = mock(LinkedList.class);

        //stubbing using built-in anyInt() argument matcher
        when(mockedList.get(anyInt())).thenReturn("test 3 - element");

        //following prints "element"
        System.out.println(mockedList.get(999));

        //you can also verify using an argument matcher
        verify(mockedList).get(anyInt());


        //verify(mockedList).get(0);

    }

}
