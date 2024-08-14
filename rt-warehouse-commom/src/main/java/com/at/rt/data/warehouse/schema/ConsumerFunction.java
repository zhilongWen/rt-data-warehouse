package com.at.rt.data.warehouse.schema;

import java.io.Serializable;
import java.util.function.Function;

/**
 * @author wenzhilong
 */
public interface ConsumerFunction<T, R> extends Function<T, R>, Serializable {
}
