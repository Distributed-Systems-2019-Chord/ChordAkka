package org.distributed.systems.chord.util;

import com.typesafe.config.Config;
import org.distributed.systems.chord.util.impl.HashUtil;

import java.net.InetAddress;
import java.util.UUID;

public class Util {

    /**
     * Returns the akka address for the central node.
     * This is either fed by config variables, or by enviromental variables.
     *
     * @return
     */
    public static String getCentralNodeAddress(Config config) {
        String centralEntityAddress = config.getString("myapp.centralEntityAddress");
        String centralEntityAddressPort = config.getString("myapp.centralEntityPort");
        if (System.getenv("CHORD_CENTRAL_NODE") != null) {
            centralEntityAddress = System.getenv("CHORD_CENTRAL_NODE");
            centralEntityAddressPort = System.getenv("CHORD_CENTRAL_NODE_PORT");
            try {
                InetAddress address = InetAddress.getByName(centralEntityAddress);
                centralEntityAddress = address.getHostAddress();
                System.out.println(address.getHostAddress());
            } catch (Exception e) {
                // TODO: need to handle
            }
        }

        return "akka://ChordNetwork@" + centralEntityAddress + ":" + centralEntityAddressPort + "/user/ChordActor";
    }


    public static long getNodeId() {
        long envVal;
        HashUtil hashUtil = new HashUtil();
        if (System.getenv("NODE_ID") == null) {
            envVal = hashUtil.hash(UUID.randomUUID().toString());
        } else {
            envVal = Long.parseLong(System.getenv("NODE_ID"));
        }

        return envVal;
    }

    public static String getNodeType(Config config) {
        String nodeType = config.getString("myapp.nodeType");

        if (System.getenv("CHORD_NODE_TYPE") != null) {
            nodeType = System.getenv("CHORD_NODE_TYPE");
        }
        return nodeType;
    }

}
