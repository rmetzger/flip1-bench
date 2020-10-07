package com.ververica.utilz;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;

import com.ververica.TPCHQuery3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class KillerClient {
    private static final Logger LOG = LoggerFactory.getLogger(KillerClient.class);
   /* public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        AkkaRpcService rpcService = AkkaRpcServiceUtils.createRemoteRpcService(config, "localhost", "6667-9000", "localhost", Optional.empty());

        CompletableFuture<KillerServerInterface> killerServiceFuture = rpcService.connect("akka.tcp://flink@localhost:6666/user/rpc/78f20bbd-cbfc-4f57-aa48-7962e7ace8c3", KillerServerInterface.class);
        KillerServerInterface killerService = killerServiceFuture.get();

        UUID myId = UUID.randomUUID();
        CompletableFuture<KillerServerInterface.Action> registerFuture = killerService.register(myId, Time.hours(24));
        KillerServerInterface.Action action = registerFuture.get();
        if (action == KillerServerInterface.Action.IGNORE) {
            System.out.println("Ignoring");
        } else {
            System.out.println("killing");
            System.exit(0);
        }
    } */

    public static <T> DataSet<T> addMapper(DataSet<T> inSet, String killerRpcEndpoint) {
        return inSet.map(new KillerClientMapper<>(killerRpcEndpoint));
    }

    private static class KillerClientMapper<T> extends RichMapFunction<T, T> {
        private final Time timeout;
        private UUID myId;
        private final String killerRpcEndpoint;
        private KillerServerInterface killerService;
        private CompletableFuture<KillerServerInterface.Action> registerFuture;
        private static AkkaRpcService rpcService;

        public KillerClientMapper(String killerRpcEndpoint) {
            this.killerRpcEndpoint = killerRpcEndpoint;
            timeout = Time.hours(24);
            rpcService = null;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            myId = UUID.randomUUID();
            Configuration config = new Configuration();
            synchronized (KillerClientMapper.class) {
                if (rpcService == null) {
                    rpcService = AkkaRpcServiceUtils.createRemoteRpcService(config, "localhost", "12000-14000", "localhost", Optional.empty());
                    LOG.info("RPC service started");
                }
            }

            CompletableFuture<KillerServerInterface> killerServiceFuture = rpcService.connect(killerRpcEndpoint, KillerServerInterface.class);
            killerService = killerServiceFuture.get();

            registerFuture = killerService.register(myId, timeout);
            LOG.info("Registered to killer service as " + myId +" subtask = " + getRuntimeContext().getIndexOfThisSubtask() + " name = " + getRuntimeContext().getTaskNameWithSubtasks());
        }

        @Override
        public T map(T value) throws Exception {
            if (registerFuture.isDone()) {
                if (registerFuture.get() == KillerServerInterface.Action.KILL) {
                    LOG.info("Kill requested");
                    throw new RuntimeException("Kill requested");
                } else {
                    // LOG.warn("Unexpected future result: " + registerFuture.get());
                }
            }
            return value;
        }

        @Override
        public void close() throws Exception {
            super.close();
            LOG.info("Unregistering from killer service");
            killerService.unregister(myId, timeout).get();
        }
    }
}
