package com.aws.greengrass;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

// TODO: Use dynamic dispatch to call the methods below. Currently this class only support linux.
//  this should eventually be integrated as part of OTF and removed from here
public class NetworkUtils {
    protected static final String[] NETWORK_PORTS = {"443", "8888", "8889"};
    private static final String ENABLE_OPTION = "--insert";
    private static final String DISABLE_OPTION = "--delete";
    private static final String IPTABLE_COMMAND_STR = "sudo iptables %s OUTPUT -p tcp --dport %s -j REJECT && " +
            "sudo iptables %s INPUT -p tcp --sport %s -j REJECT";

    public static void disconnectNetwork() throws InterruptedException, IOException {
        modifyInterfaceUpDownPolicy(IPTABLE_COMMAND_STR, ENABLE_OPTION, "connection-loss", NETWORK_PORTS);
    }

    public static void recoverNetwork() throws InterruptedException, IOException {
        modifyInterfaceUpDownPolicy(IPTABLE_COMMAND_STR, DISABLE_OPTION, "connection-recover", NETWORK_PORTS);
    }

    private static void modifyInterfaceUpDownPolicy(String iptableCommandString, String option, String eventName, String... ports) throws InterruptedException,
            IOException {
        for (String port : ports) {
            new ProcessBuilder().command("sh", "-c", String.format(iptableCommandString, option, port, option, port))
                    .start().waitFor(2, TimeUnit.SECONDS);
        }
    }
}
