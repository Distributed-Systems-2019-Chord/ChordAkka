package org.distributed.systems.chord.test;

import akka.actor.testkit.typed.javadsl.TestKitJunitResource;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.testkit.javadsl.TestKit;
import org.distributed.systems.chord.actors.Node;
import org.distributed.systems.chord.model.ChordNode;
import org.distributed.systems.chord.model.finger.Finger;
import org.distributed.systems.chord.model.finger.FingerInterval;
import org.distributed.systems.chord.model.finger.FingerTable;
import org.distributed.systems.chord.service.FingerTableService;
import org.junit.*;

import java.util.ArrayList;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.AbstractActor;
import java.time.Duration;

public class AkkaFindSuccesorTest {

    /*@Test
    public void closestPrecedingFinger(){

        //Node entries for in the finger tables
        ChordNode node0 = new ChordNode(0L, "localhost", 8080);
        ChordNode node1 = new ChordNode(1L, "localhost",8081);
        ChordNode node3 = new ChordNode(3L, "localhost", 8082);

        //Table for first test
        FingerTableService service = new FingerTableService();
        FingerTable table0 = new FingerTable(new ArrayList<>(3),0);
        table0.addFinger(new Finger(1L, new FingerInterval(1L,2L), node1));
        table0.addFinger(new Finger(2L, new FingerInterval(2L,4L), node3));
        table0.addFinger(new Finger(4L, new FingerInterval(4L,0L), node0));

        //Table for second and third test
        FingerTable table3 = new FingerTable(new ArrayList<>(3), 0);
        table3.addFinger(new Finger(4L, new FingerInterval(4L,5L), node0));
        table3.addFinger(new Finger(5L, new FingerInterval(5L,7L), node0));
        table3.addFinger(new Finger(7L, new FingerInterval(7L,3L), node0));


        // Suppose node 3 wants to find the successor of key 1
        // Closest_preceding_finger will lookup in fingertable of node 3 and will find that key 1 falls within
        // key interval 7 and 3. Expected result of this test is that centralNode_0 is the return value of
        // closest_preceding_finger

        // TODO: Also make a test if the id cannot be found in the finger table.
        // TODO: Uncomment this when closestPrecedingFinger is implemented.
        //Test 1
        long expected1 = 3L;
        service.setFingerTable(table0);
        //long actual1 = service.closestPrecedingFinger(node0,4).getId;
        //Assert.assertEquals(expected1,actual1);


        //Test 2
        service.setFingerTable(table3);
        long expected2 = 0L;
        //long actual2 = service.closestPrecedingFinger(node3,1).getId();
        //Assert.assertEquals(expected2, actual2);


        //Test 3, uses the same finger table as test 2, should return the node's own id, 3.
        long expected3 = 3L;
        //long actual3 = service.closestPrecedingFinger(node3,3).getId();
        //Assert.assertEquals(expected3,actual3);
    }*/
}
