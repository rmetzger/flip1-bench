package com.ververica.utilz;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.math3.distribution.ExponentialDistribution;
import org.apache.commons.math3.random.Well19937c;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KillerServer extends RpcEndpoint implements KillerServerInterface {
    private static final Logger LOG = LoggerFactory.getLogger(KillerServer.class);

    private final Map<UUID, CompletableFuture<Action>> registeredClients = new HashMap<>();

    private final Random RNG = new Random();

    private KillerServer(RpcService rpcService, double meanKillFrequency) {
        super(rpcService);
        if (meanKillFrequency > 0) {
            new Thread(() -> {
                try {
                    LOG.info("Starting KillerServer with a mean kill frequency of {}", meanKillFrequency);

                    ExponentialDistribution dist = new ExponentialDistribution(new Well19937c(2), meanKillFrequency);
                    while (true) {
                        long nextKillSeconds = Math.round(dist.sample());
                        LOG.info("Sleeping {} seconds until next kill", nextKillSeconds);
                        Thread.sleep(nextKillSeconds * 1000);
                        LOG.info("Checking for client to kill: {}", registeredClients.size());
                        synchronized (registeredClients) {
                            UUID[] clients = registeredClients.keySet().toArray(new UUID[]{});
                            if (clients.length <= 1) {
                                continue;
                            }

                            UUID toKill = clients[RNG.nextInt(clients.length)];
                            LOG.info("Selected client to kill {}", toKill);
                            CompletableFuture<Action> toKillFuture = registeredClients.remove(toKill);
                            toKillFuture.complete(Action.KILL);
                        }
                    }
                } catch (Throwable e) {
                    LOG.warn("Thread died", e);
                }
            }).start();
        }
    }

    @Override
    public CompletableFuture<Action> register(UUID client, Time timeout) {
        synchronized (registeredClients) {
            if (registeredClients.containsKey(client)) {
                throw new RuntimeException("Client " + client + " has already been registered");
            }
            LOG.info("Register client {}", client);
            CompletableFuture<Action> future = new CompletableFuture<>();
            registeredClients.put(client, future);
            return future;
        }
    }

    @Override
    public CompletableFuture<Acknowledge> unregister(UUID client, Time timeout) {
        synchronized (registeredClients) {
            LOG.info("Unregister client {}", client);
            CompletableFuture<Action> future = registeredClients.remove(client);
            if (future != null) {
                future.complete(Action.IGNORE);
            } else {
                LOG.warn("This client was not known");
            }
            return CompletableFuture.completedFuture(Acknowledge.get());
        }
    }

    public static String launchServer(double meanKillFrequency) throws Exception {
        Configuration config = new Configuration();
        AkkaRpcService rpcService = AkkaRpcServiceUtils.createRemoteRpcService(config, "localhost", "6666", "localhost", Optional.empty());

        KillerServer srv = new KillerServer(rpcService, meanKillFrequency);
        srv.start();
        return srv.getAddress();
    }

    public static void main(String[] args) throws Exception {
        LOG.info("Server reachable at " + launchServer(10));
    }
}
