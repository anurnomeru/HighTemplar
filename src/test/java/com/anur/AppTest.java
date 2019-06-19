package com.anur;

import java.util.concurrent.CountDownLatch;
import com.anur.ht.lock.HtReentrantLock;
import static org.junit.Assert.assertTrue;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {

    @Test
    public void ReentrantLockTest() throws InterruptedException {

        CountDownLatch cdl = new CountDownLatch(2);
        Runnable runnable = () -> {
            HtReentrantLock htReentrantLock = new HtReentrantLock("Anur", new ZkClient("127.0.0.1"));
            htReentrantLock.lock();
            htReentrantLock.lock();

            System.out.println("----------get lock----------");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            htReentrantLock.unLock();
            System.out.println("----------un lock once----------");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            htReentrantLock.unLock();
            System.out.println("----------un lock twice----------");
            cdl.countDown();
        };

        new Thread(runnable).start();
        new Thread(runnable).start();

        cdl.await();
    }

    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue() {
        assertTrue(true);
    }
}
