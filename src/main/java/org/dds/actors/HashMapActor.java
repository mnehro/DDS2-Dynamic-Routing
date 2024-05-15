package org.dds.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import org.dds.messages.*;

import java.util.HashMap;
import java.util.Map;

public class HashMapActor extends AbstractActor {
    private final LoggingAdapter LOG = Logging.getLogger(getContext().getSystem(), this);

    private static final Map<Integer, Double> fakeDB = new HashMap<>();

    public static Props props() {
        return Props.create(HashMapActor.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(AddMessage.class, this::add)
                .match(PutAllMessage.class, this::putALL)
                .match(ReplaceMessage.class, this::replace)
                .match(RemoveMessage.class, this::remove)
                .match(ClearMessage.class, this::clear)
                .match(ListMessage.class, this::list)
                .matchAny(o -> LOG.info("Unknown message type"))
                .build();
    }

    private boolean keyExists(Integer key) {
        return fakeDB.containsKey(key);
    }
    private void add(AddMessage message) {
        if (this.keyExists(message.key())) {
            LOG.info(
                    "Key {} already exists in the DB (Add Operation), from {}, on actor {}",
                    message.key(), getSender(), self().path()
            );
            return;
        }
        fakeDB.put(message.key(), message.value());
        LOG.info(
                "Added value with key {} and value {} successfully (Add Operation), from {}, on actor {}",
                message.key(), message.value(), getSender(), self().path()
        );
    }

    private void putALL(PutAllMessage message) {
        message.newData().forEach(fakeDB::putIfAbsent);

        LOG.info(
                "Added values with keys {} and values {} successfully (Put ALl Operation), from {}, on actor {}",
                message.newData().keySet(), message.newData().values(), getSender(),self().path()
        );
    }

    private void replace(ReplaceMessage message) {
        if (this.keyExists(message.key())) {
            fakeDB.replace(message.key(), message.newValue());
            LOG.info(
                    "Replaced value with key {} with new value {} successfully (Replace Operation), from {}, on actor {}",
                    message.key(), message.newValue(), getSender(), self().path()
            );
            return;
        }
        LOG.info(
                "Key {} Does not exist (Replace Operation), from {}, on actor {}",
                message.key(), getSender(), self().path()
        );
    }

    private void remove(RemoveMessage message) {
        if (this.keyExists(message.key())) {
            fakeDB.remove(message.key());
            LOG.info(
                    "Removed value with key {} successfully (Remove Operation), from {}, on actor {}",
                    message.key(), getSender(), self().path()
            );
            return;
        }
        LOG.info(
                "Key {} Does not exist (Remove Operation), from {}, on actor {}",
                message.key(), getSender(),  self().path()
        );
    }

    private void clear(ClearMessage message) {
        fakeDB.clear();
        LOG.info(
                "Cleared the FakeDB successfully (Clear Operation),from {}, on actor {}", getSender() , self().path()
        );
    }

    private void list(ListMessage message) {
        LOG.info(
                "(List Operation), from {}, on actor {}", getSender() , self().path()
        );
        if (!fakeDB.isEmpty()) {
            fakeDB.forEach((key, value) -> LOG.info(
                    "Key {} | Value {} ", key, value
            ));
        } else {
            LOG.info(
                    "DB is empty"
            );
        }

    }
}
