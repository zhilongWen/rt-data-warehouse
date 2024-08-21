create
temporary view order_detail_coupon asselect     data['order_detail_id'] order_detail_id,     data['coupon_id'] coupon_id from topic_db where `table`='order_detail_coupon' and `type`='insert'
