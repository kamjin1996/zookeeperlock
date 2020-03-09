package com.kam.zookeeperlock;

public class LockException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public LockException(Exception e) {
        super(e);
    }

    public LockException(String e) {
        super(e);
    }
}
