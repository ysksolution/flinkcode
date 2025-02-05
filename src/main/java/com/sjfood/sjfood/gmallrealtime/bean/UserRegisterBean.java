package com.sjfood.sjfood.gmallrealtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class UserRegisterBean {
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 注册用户数
    Long registerCt;
    // 时间戳
    Long ts;
}