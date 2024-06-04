package com.sjfood.sjfood.gmallrealtime.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @Author: YSKSolution
 * @Date: 2022/11/16/14:34
 * @Package_name: com.atguigu.gmallrealtime.annotation
 */

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface NotSink {

}
