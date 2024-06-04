package com.sjfood.sjfood.gmallrealtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/11/16:07
 * @Package_name: com.atguigu.gmallrealtime.bean
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Person {

    private String name;
    public int age;


    private void privateArgShow(String name){
        System.out.println("=================== privateArgShow method ==================");
    }

    private void privateShow(){
        System.out.println("=================== private method ==================");
    }


    public void publicShow(){
        System.out.println("=================== public method =========================");
    }

    public void publicArgShow(int age){
        System.out.println("=================== public method =========================");
    }
}
