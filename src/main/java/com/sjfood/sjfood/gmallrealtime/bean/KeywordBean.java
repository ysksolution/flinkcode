package com.sjfood.sjfood.gmallrealtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordBean {
    // 窗口起始时间
    private String stt;

    // 窗口结束时间
    private String edt;

    // 关键词
    private String keyword;

    // 关键词出现频次
    private Long keywordCount;


    // 时间戳：结果的统计时间
    private Long ts;
}