package com.ververica.utilz;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class KillerClient {
    private static final Logger LOG = LoggerFactory.getLogger(KillerClient.class);

    private final Time timeout;
    private UUID myId;
    private final String killerRpcEndpoint;
    private KillerServerInterface killerService;
    private CompletableFuture<KillerServerInterface.Action> registerFuture;
    private static AkkaRpcService rpcService;
    private static int runningClients = 0;
    private static Random RNG = new Random();

    public KillerClient(String killerRpcEndpoint) {
        this.killerRpcEndpoint = killerRpcEndpoint;
        timeout = Time.hours(24);
        rpcService = null;
    }

    public void open(RuntimeContext context) throws Exception {
        myId = UUID.randomUUID();
        Configuration config = new Configuration();
        synchronized (KillerClient.class) {
            if (rpcService == null) {
                String portRangeStart = String.valueOf(12000 + RNG.nextInt(1500));
                rpcService = AkkaRpcServiceUtils.createRemoteRpcService(config, "localhost", portRangeStart+ "-14000", "localhost", Optional.empty());
                LOG.info("RPC service started");
            }
            CompletableFuture<KillerServerInterface> killerServiceFuture = rpcService.connect(killerRpcEndpoint, KillerServerInterface.class);
            runningClients++;
            LOG.info("Connected to killer RPC endpoint: Running clients {}", runningClients);
            killerService = killerServiceFuture.get();
        }

        registerFuture = killerService.register(myId, timeout);
        LOG.info("Registered to killer service as " + myId +" subtask = " + context.getIndexOfThisSubtask() + " name = " + context.getTaskNameWithSubtasks());
    }

    public void maybeFail() throws ExecutionException, InterruptedException {
        if (registerFuture.isDone()) {
            if (registerFuture.get() == KillerServerInterface.Action.KILL) {
                LOG.info("Kill requested");
                throw new RuntimeException("Kill requested");
            } else {
                LOG.warn("Unexpected future result: " + registerFuture.get());
            }
        }
    }

    public void close() throws ExecutionException, InterruptedException {
        synchronized (KillerClient.class) {
            killerService.unregister(myId, timeout).get();
            runningClients--;
            LOG.info("Unregistering from killer service. Running clients {}", runningClients);
            if (runningClients == 0) {
                LOG.info("0 running clients. Stopping RPC service.");
                rpcService.stopService().get();
                rpcService = null;
            }
        }
    }
}
