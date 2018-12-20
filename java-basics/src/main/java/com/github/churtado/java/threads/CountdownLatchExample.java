package com.github.churtado.java.threads;

import java.sql.Time;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Going to show how a countdown latch works
 */

public class CountdownLatchExample {

    // let's create a Worker and use a CountDownLatch field to signal when it has completed

    public class Worker implements Runnable {

        private List<String> outputScraper;
        private CountDownLatch countDownLatch;

        public Worker(List<String> outputScraper, CountDownLatch countDownLatch) {
            this.outputScraper = outputScraper;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            doSomeWork();
            outputScraper.add("Counted down");
            countDownLatch.countDown();
        }

        private void doSomeWork(){
        }

    }

    public class BrokenWorker implements Runnable {

        private List<String> outputScraper;
        private CountDownLatch countDownLatch;

        public BrokenWorker(List<String> outputScraper, CountDownLatch countDownLatch) {
            this.outputScraper = outputScraper;
            this.countDownLatch = countDownLatch;
        }

        @Override
        public void run() {
            if(true) {
                throw new RuntimeException("I'm a broken worker");
            }
            countDownLatch.countDown();
            outputScraper.add("Counted down");
        }
    }

    @Test
    public void whenParallelProcessing_thenMainThreadWillBlockUntilCompletion() throws InterruptedException {
        List<String> outputScraper = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch countDownLatch = new CountDownLatch(5);
        List<Thread> workers = Stream
                .generate(() -> new Thread(new Worker(outputScraper, countDownLatch)))
                .limit(5)
                .collect(toList());

        workers.forEach(Thread::start);
        countDownLatch.await();
        outputScraper.add("Latch released");

        List<String> expected = Arrays.asList(
                "Counted down",
                "Counted down",
                "Counted down",
                "Counted down",
                "Counted down",
                "Latch released"
        );

        assertEquals(outputScraper, expected);
    }

    @Test
    public void whenFailingToParallelProcess_thenMainThreadShouldNotGetStuck() throws InterruptedException {
        List<String> outputScraper = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch countDownLatch = new CountDownLatch(5);
        List<Thread> workers = Stream
                .generate(() -> new Thread(new BrokenWorker(outputScraper, countDownLatch)))
                .limit(5)
                .collect(toList());

        workers.forEach(Thread::start); // uses a Stream object so they can be started in parallel
        boolean completed = countDownLatch.await(3L, TimeUnit.SECONDS);
        assertEquals(completed, false);
    }
}
