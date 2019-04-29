package com.mym.practice.lock;

import org.junit.Before;
import org.junit.Test;

public class LockTest {

    @Before
    public void init(){
        System.out.println("init ...");
    }

    @Test
    public void testSyncLock(){
        boolean result = SyncUtils.getSyncLock("setnxKey", 10);
        System.out.println("get syncLock result:" + result);
    }

    @Test
    public void testSyncLockNew(){
        boolean result = SyncUtils.getSyncLockNewNx("setKey", 20);
        System.out.println("get syncLock new result:" + result);
    }

}
