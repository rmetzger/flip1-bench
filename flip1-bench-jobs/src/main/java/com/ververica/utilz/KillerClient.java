package com.ververica.utilz;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
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
                rpcService = AkkaRpcServiceUtils.createRemoteRpcService(config, "localhost", "12000-14000", "localhost", Optional.empty());
                LOG.info("RPC service started");
            }
        }
        CompletableFuture<KillerServerInterface> killerServiceFuture = rpcService.connect(killerRpcEndpoint, KillerServerInterface.class);
        killerService = killerServiceFuture.get();

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
        LOG.info("Unregistering from killer service");
        killerService.unregister(myId, timeout).get();
    }
}
