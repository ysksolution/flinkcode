package com.sjfood.sjfood.gmallrealtime.bean;


import com.sjfood.sjfood.gmallrealtime.annotation.NotSink;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@Builder
public class TradeSkuOrderBean {
    @NotSink
    String orderDetailId;
    // 窗口起始时间
    String stt;
    // 窗口结束时间
    String edt;
    // 品牌 ID
    String trademarkId;
    // 品牌名称
    String trademarkName;
    // 一级品类 ID
    String category1Id;
    // 一级品类名称
    String category1Name;
    // 二级品类 ID
    String category2Id;
    // 二级品类名称
    String category2Name;
    // 三级品类 ID
    String category3Id;
    // 三级品类名称
    String category3Name;
    // sku_id
    String skuId;
    // sku 名称
    String skuName;
    // spu_id
    String spuId;
    // spu 名称
    String spuName;
    // 原始金额
    BigDecimal originalAmount;
    // 活动减免金额
    BigDecimal activityAmount;
    // 优惠券减免金额
    BigDecimal couponAmount;
    // 下单金额
    BigDecimal orderAmount;
    // 时间戳
    Long ts;
}