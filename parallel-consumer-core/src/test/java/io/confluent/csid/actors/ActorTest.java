package io.confluent.csid.actors;

import io.confluent.csid.utils.TimeUtils;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Future;

import static io.confluent.parallelconsumer.ManagedTruth.assertThat;

/**
 * @see Actor
 */
@Slf4j
class ActorTest {

    public static final String MESSAGE = "tell";
    Greeter greeter = new Greeter();
    Actor<Greeter> actor = new Actor<>(TimeUtils.getClock(), greeter);

    // todo get TG working with Greeter class
    @Data
    public static class Greeter {
        public static final String PREFIX = "kiwi-";
        String told = "";

        public String greet(String msg) {
            return PREFIX + msg;
        }
    }

    @Test
    void tell() {
        actor.tell(g -> g.setTold("1"));
        actor.tell(g -> g.setTold("2"));
        actor.processBounded();
        assertThat(greeter.getTold()).isEqualTo("2");
    }

    @Test
    void tellImmediately() {
        actor.tell(g -> g.setTold("1"));
        actor.tellImmediately(g -> g.setTold("2"));
        actor.processBounded();
        assertThat(greeter.getTold()).isEqualTo("1");
    }

    @SneakyThrows
    @Test
    void ask() {
        Future<String> tell = actor.ask(g -> g.greet(MESSAGE));
        actor.processBounded();
        String s = tell.get();
        assertThat(s).isEqualTo(Greeter.PREFIX + MESSAGE);
    }

    @Test
    void processBlocking() {
        //todo
    }
}