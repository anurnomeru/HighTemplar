package com.anur;

import java.util.concurrent.CountDownLatch;
import com.anur.ht.common.HtZkClient;
import com.anur.ht.lock.HtReentrantLock;
import com.anur.ht.lock.HtReentrantReadWriteLock;
import com.anur.ht.lock.HtReentrantReadWriteLock.ReadLock;
import com.anur.ht.lock.HtReentrantReadWriteLock.WriteLock;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {

    @Test
    public void ReentrantLockTest() throws InterruptedException {

        CountDownLatch cdl = new CountDownLatch(2);
        HtReentrantLock htReentrantLock = new HtReentrantLock("-Anur-", new HtZkClient("127.0.0.1"));

        Runnable runnable = () -> {
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

    @Test
    public void ReetrantReadWriteLockTest() throws InterruptedException {

        CountDownLatch cdl = new CountDownLatch(2);
        HtReentrantReadWriteLock wrl = new HtReentrantReadWriteLock("Anur-test", new HtZkClient("127.0.0.1"));
        ReadLock readLock = wrl.readLock();
        WriteLock writeLock = wrl.writeLock();

        Runnable runnable = () -> {
            try {
                System.out.println("----------try get r-lock----------");
                readLock.lock();
                System.out.println("----------try get r-lock success----------");

                System.out.println("----------try get w-lock----------");
                writeLock.lock();
                System.out.println("----------try get w-lock success----------");

                System.out.println("----------try get r-lock----------");
                readLock.lock();
                System.out.println("----------try get r-lock success----------");

                System.out.println("----------try get r-lock----------");
                readLock.lock();
                System.out.println("----------try get r-lock success----------");

                System.out.println("----------try get w-lock----------");
                writeLock.lock();
                System.out.println("----------try get w-lock success----------");

                Thread.sleep(2000);

                readLock.unLock();
                System.out.println("----------un lock readLock 1----------");
                Thread.sleep(1000);

                writeLock.unLock();
                System.out.println("----------un lock writeLock 1----------");
                Thread.sleep(1000);

                readLock.unLock();
                System.out.println("----------un lock readLock 2----------");
                Thread.sleep(1000);

                readLock.unLock();
                System.out.println("----------un lock readLock 3----------");
                Thread.sleep(1000);

                writeLock.unLock();
                System.out.println("----------un lock writeLock 2----------");
                Thread.sleep(1000);

                cdl.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        };

        new Thread(runnable).start();
//        new Thread(runnable).start();

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
