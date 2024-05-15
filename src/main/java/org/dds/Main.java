package org.dds;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.routing.ConsistentHashingPool;
import akka.routing.ConsistentHashingRouter;
import org.dds.actors.HashMapActor;
import org.dds.messages.*;

import java.time.Duration;
import java.util.*;

public class Main {
    private final static String ACTOR_SYSTEM = "DDS_1";
    private final static String HASH_MAP_ACTOR = "hashMapActor";

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create(ACTOR_SYSTEM);

        final ConsistentHashingRouter.ConsistentHashMapper hashMapper =
                message -> switch (message) {
                    case AddMessage addMessage -> addMessage.key();
                    case ReplaceMessage replaceMessage -> replaceMessage.key();
                    case RemoveMessage removeMessage -> removeMessage.key();
                    case null, default -> -1;
                };


        ActorRef consistentHashingPool = system.actorOf(
                new ConsistentHashingPool(10).withHashMapper(hashMapper).props(HashMapActor.props())
                , HASH_MAP_ACTOR);

        Map<Integer, Double> putAll = new HashMap<>();
        putAll.put(8, 200.2);
        putAll.put(3, 500.2);
        putAll.put(4, 400.2);

        List<AddMessage> addMessages = new ArrayList<>();

        putAll.forEach((key, value) -> addMessages.add(new AddMessage(key, value)));

        List<Message> messages = new ArrayList<>();
        messages.add(new AddMessage(3, 22.2));
        messages.add(new AddMessage(2, 200.2));
        messages.add(new AddMessage(3, 30.2));
        messages.add(new AddMessage(1, 22.2));
        messages.add(new AddMessage(1, 10.2));
        messages.add(new AddMessage(1, 40.2));
        messages.add(new AddMessage(1, 11.2));
        messages.add(new AddMessage(3, 222.2));
        messages.add(new ReplaceMessage(4, 800.2));
        messages.add(new RemoveMessage(1));
        messages.addAll(addMessages);
//        messages.add( new ClearMessage());

        messages.forEach(message -> consistentHashingPool.tell(message, system.guardian()));


        system.scheduler().scheduleWithFixedDelay(
                Duration.ofSeconds(1),
                Duration.ofSeconds(3),
                consistentHashingPool,
                new ListMessage(),
                system.dispatcher(),
                system.guardian()
        );

    }
}