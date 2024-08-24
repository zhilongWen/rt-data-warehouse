package com.at.rt.data.warehouse.dws;

import com.at.rt.data.warehouse.bean.TradeSkuOrderBean;
import com.at.rt.data.warehouse.utils.JSONWrapper;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.*;

import java.io.IOException;
import java.math.BigDecimal;

public class MMk {
    public static void main(String[] args) throws JsonProcessingException {


        String str = "{\"id\":\"13518\",\"order_id\":\"5079\",\"user_id\":\"351\",\"sku_id\":\"33\",\"sku_name\":\"香奈儿（Chanel）女士香水5号香水 粉邂逅柔情淡香水EDT 粉邂逅淡香水35ml\",\"province_id\":\"20\",\"activity_id\":null,\"activity_rule_id\":null,\"coupon_id\":null,\"date_id\":\"2024-08-22\",\"create_time\":\"2024-08-22 23:53:51\",\"sku_num\":\"3\",\"split_original_amount\":\"1464.0000\",\"split_activity_amount\":null,\"split_coupon_amount\":null,\"split_total_amount\":\"1464.0\",\"ts\":1724342031}";

        System.out.println(str);

        TradeSkuOrderBean tradeSkuOrderBean = JSONWrapper.parseObject(str, TradeSkuOrderBean.class);
        System.out.println(tradeSkuOrderBean);

        tradeSkuOrderBean.setOrderAmount(tradeSkuOrderBean.getOriginalAmount().negate());
        System.out.println(tradeSkuOrderBean);

        tradeSkuOrderBean.setActivityReduceAmount(tradeSkuOrderBean.getActivityReduceAmount() == null ? BigDecimal.ZERO : tradeSkuOrderBean.getActivityReduceAmount().negate());
        System.out.println(tradeSkuOrderBean);


//        ObjectMapper objectMapper = new ObjectMapper();
//        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
//        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//        TradeSkuOrderBean tradeSkuOrderBean1 = objectMapper.readValue(str, TradeSkuOrderBean.class);
//        System.out.println(tradeSkuOrderBean1);


//        System.out.println("================================");
//
//        String jsonString = "{\"id\": 123, \"name\": \"Sample Order\"}";
//
//        try {
//            ObjectMapper objectMapper1 = new ObjectMapper();
//            OrderDetail orderDetail = JSONWrapper.parseObject(str, OrderDetail.class); //objectMapper1.readValue(jsonString, OrderDetail.class);
//            System.out.println(orderDetail);
//
//            System.out.println("Order Detail ID: " + orderDetail.getOrderDetailId());
//            System.out.println("Name: " + orderDetail.getName());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        String jsonString = "{\"activityReduceAmount\": null,\"name\":\"Discount\"}";
////        String jsonString = "{\"activityReduceAmount\": \"\", \"name\": \"Discount\"}";
//
//
//        try {
//            ObjectMapper objectMapper = new ObjectMapper();
//            Activity activity = objectMapper.readValue(jsonString, Activity.class);
//
//            System.out.println("Activity Reduce Amount: " + activity.getActivityReduceAmount());
//            System.out.println("Name: " + activity.getName());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    @ToString
    static class OrderDetail {

        @JsonProperty("id")
        private String orderDetailId;
        private String name;
    }

    static class Activity {
        @JsonDeserialize(using = BigDecimalNullOrEmptyToZeroDeserializer.class)
        private BigDecimal activityReduceAmount;

        private String name;

        // Getters and setters
        public BigDecimal getActivityReduceAmount() {
            return activityReduceAmount;
        }

        public void setActivityReduceAmount(BigDecimal activityReduceAmount) {
            this.activityReduceAmount = activityReduceAmount;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return super.toString();
        }
    }

    static class BigDecimalNullOrEmptyToZeroDeserializer extends JsonDeserializer<BigDecimal> {
        @Override
        public BigDecimal deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

            if (p == null) {
                return BigDecimal.ZERO;
            }

            String value = p.getText();
            if (value == null || value.isEmpty()) {
                return BigDecimal.ZERO;
            }
            return new BigDecimal(value);
        }
    }
}
