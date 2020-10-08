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
    private Logger log;

    private final Time timeout = Time.hours(24);
    private UUID instanceId;
    private final String killerRpcEndpoint;
    private KillerServerInterface killerService;
    private CompletableFuture<KillerServerInterface.Action> registerFuture;
    private static final Object lock = new Object();
    private static AkkaRpcService rpcService = null;
    private static int runningClients = 0;
    private static final Random RNG = new Random();
    private boolean registered = false;

    public KillerClient(String killerRpcEndpoint) {
        this.killerRpcEndpoint = killerRpcEndpoint;
    }

    public void open(RuntimeContext context) throws Exception {
        instanceId = UUID.randomUUID();
        if(log != null){
            throw new RuntimeException("open() is not expected to be called multiple times");
        }
        log = LoggerFactory.getLogger(this.toString() + "-" + instanceId);
        Configuration config = new Configuration();
        synchronized (lock) {
            if (rpcService == null) {
                String portRangeStart = String.valueOf(12000 + RNG.nextInt(1500));
                rpcService = AkkaRpcServiceUtils.createRemoteRpcService(config, "localhost", portRangeStart+ "-14000", "localhost", Optional.empty());
                log.info("RPC service started. On {} with classloader {} and {} guarded by lock {}", this, KillerClient.class.getClassLoader(), this.getClass().getClassLoader(), lock);
                context.registerUserCodeClassLoaderReleaseHookIfAbsent("closeRPC", () -> {
                    try {
                        synchronized (lock) {
                            if (rpcService == null) {
                                return;
                            }
                            log.info("Unregistering RPC service, despite having {} running clients.", runningClients);
                            rpcService.stopService().get();
                            rpcService = null;
                        }
                    } catch (Throwable e) {
                        log.warn("Error while stopping RPC service", e);
                    }
                });
            }
            CompletableFuture<KillerServerInterface> killerServiceFuture = rpcService.connect(killerRpcEndpoint, KillerServerInterface.class);
            runningClients++;
            log.info("Connected to killer RPC endpoint: Running clients {}", runningClients);
            killerService = killerServiceFuture.get();
            registered = true;
            registerFuture = killerService.register(instanceId, timeout);
            log.info("Registered to killer service as " + instanceId +" subtask = " + context.getIndexOfThisSubtask() + " name = " + context.getTaskNameWithSubtasks());
        }
    }

    public void maybeFail() throws ExecutionException, InterruptedException {
        if (registerFuture.isDone()) {
            if (registerFuture.get() == KillerServerInterface.Action.KILL) {
                log.info("Kill requested");
                close(); // closing, just to make sure.
                throw new RuntimeException("Kill requested");
            } else {
                log.warn("Unexpected future result: " + registerFuture.get());
            }
        }
    }

    public void close() throws ExecutionException, InterruptedException {
        synchronized (lock) {
            if (registered) {
                killerService.unregister(instanceId, timeout).get();
                runningClients--;
                log.info("Unregistering from killer service. Running clients {}", runningClients);
                if (runningClients <= 0) {
                    log.info("0 running clients. Stopping RPC service.");
                    rpcService.stopService().get();
                    rpcService = null;
                }
            }
            registered = false;
        }
    }
}
