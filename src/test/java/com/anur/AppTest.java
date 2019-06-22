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

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            htReentrantLock.unLock();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            htReentrantLock.unLock();
            cdl.countDown();
        };

        new Thread(runnable).start();
        new Thread(runnable).start();

        cdl.await();
    }

    @Test
    public void ReentrantReadWriteLockTest() throws InterruptedException {

        CountDownLatch cdl = new CountDownLatch(6);
        HtReentrantReadWriteLock wrl = new HtReentrantReadWriteLock("-Anur-WR-", new HtZkClient("127.0.0.1"));
        ReadLock readLock = wrl.readLock();
        WriteLock writeLock = wrl.writeLock();

        new Thread(() -> {
            readLock.lock();

            try {
                System.out.println("1 - 获取到了读锁");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            readLock.unLock();
            cdl.countDown();
        }).start();
        Thread.sleep(1000);

        new Thread(() -> {
            writeLock.lock();

            try {
                System.out.println("2 - 获取到了写锁");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            writeLock.unLock();
            cdl.countDown();
        }).start();
        Thread.sleep(1000);

        new Thread(() -> {
            writeLock.lock();

            try {
                System.out.println("3 - 获取到了写锁");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            writeLock.unLock();
            cdl.countDown();
        }).start();
        Thread.sleep(1000);

        new Thread(() -> {
            readLock.lock();

            try {
                System.out.println("4 - 获取到了读锁");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            readLock.unLock();
            cdl.countDown();
        }).start();
        Thread.sleep(1000);

        new Thread(() -> {
            readLock.lock();

            try {
                System.out.println("4 - 获取到了读锁");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            readLock.unLock();
            cdl.countDown();
        }).start();
        Thread.sleep(1000);

        new Thread(() -> {
            readLock.lock();

            try {
                System.out.println("4 - 获取到了读锁");
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            readLock.unLock();
            cdl.countDown();
        }).start();
        Thread.sleep(1000);

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
