package com.at.rt.data.warehouse.exception;

/**
 * @author wenzhilong
 */
public class RTWarehouseException extends RuntimeException {

    public RTWarehouseException() {
        super();
    }

    public RTWarehouseException(String message) {
        super(message);
    }

    public RTWarehouseException(String message, Throwable cause) {
        super(message, cause);
    }

    public RTWarehouseException(Throwable cause) {
        super(cause);
    }

    protected RTWarehouseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
