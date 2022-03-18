package io.confluent.parallelconsumer.disrupter;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

@Slf4j
class VolatileTest {

    public static class TaskRunner {

        private static int number;
        private static boolean ready;


        private static class Reader extends Thread {

            @Override
            public void run() {
                while (!ready) {
                    Thread.yield();
                }

                System.out.println(number);
            }
        }

        public static void test() {


            new Reader().start();
            number = 42;
            ready = true;
        }
    }

    @Test
    void volatileTest() {

        int number = 0;
        boolean ready = false;

        Consumer<Integer> function = (int target) -> {
            while (!ready) {
                Thread.yield();
            }

            return number;
        };

        ExecutorService executorService = new ExecutorService();
        executorService.submit(function);

        Integer integer = function.get();
        number = 42;
        ready = true;
    }
}
