package com.wzl.ct.tets;

import java.util.Random;

public class randomTest {
    public static void main(String[] args) {
        for (int i = 0; i < 10; i++) {
            System.out.println(new Random().nextInt(3));
        }
    }
}
