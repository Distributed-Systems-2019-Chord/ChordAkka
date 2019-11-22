package org.distributed.systems;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import org.distributed.systems.chord.actors.Node;
import org.distributed.systems.chord.messaging.*;
import org.distributed.systems.chord.model.ChordNode;
import org.distributed.systems.chord.util.IHashUtil;
import org.distributed.systems.chord.util.impl.HashUtil;

public class ChordStart {

    public static void main(String[] args) {
        IHashUtil hashUtil = new HashUtil();

        // Create actor system
        ActorSystem system = ActorSystem.create("ChordNetwork"); // Setup actor system

        system.actorOf(Props.create(Node.class), "ChordActor");


        // Create start node

        // Create messages
//        NodeJoinMessage joinMessage = new NodeJoinMessage(node);
//        NodeLeaveMessage leaveMessage = new NodeLeaveMessage(startNode);
//        String hashId = hashUtil.hash(String.valueOf(startNode.getId()));
//        GetFingerTableMessage getFingerTableMessage = new GetFingerTableMessage(hashId);

//        String hashKey = hashUtil.hash(String.valueOf(startNode.getId()));
//        PutValueMessage putValueMessage = new PutValueMessage(hashKey, "TEST VALUE");
//        GetValueMessage getValueMessage = new GetValueMessage(hashKey);

        // Send messages to the node
//        node.tell(getFingerTableMessage, ActorRef.noSender());

        // Node is joining..
//        actor.tell(joinMessage, null);
//        node.tell(getFingerTableMessage, ActorRef.noSender());

        // Add en retrieve value
//        node.tell(putValueMessage, ActorRef.noSender());
//        node.tell(getValueMessage, ActorRef.noSender());

        // Node is leaving after sometime...
//        node.tell(leaveMessage, ActorRef.noSender());
//        node.tell(getFingerTableMessage, ActorRef.noSender());

//        system.stop(node); // Quit node

//        system.terminate(); // Terminate application
    }
}
