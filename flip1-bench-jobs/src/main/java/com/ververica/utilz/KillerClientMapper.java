package com.ververica.utilz;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class KillerClientMapper {
    private static final Logger LOG = LoggerFactory.getLogger(com.ververica.utilz.KillerClientMapper.class);
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

    public static <T> DataSet<T> appendMapper(DataSet<T> inSet, String killerRpcEndpoint) {
        return inSet.map(new InternalKillerClientMapper<>(killerRpcEndpoint));
    }

    private static class InternalKillerClientMapper<T> extends RichMapFunction<T, T> {

        private transient KillerClient killerClient;
        private final String killerRpcEndpoint;

        public InternalKillerClientMapper(String killerRpcEndpoint) {
            this.killerRpcEndpoint = killerRpcEndpoint;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            killerClient = new KillerClient(killerRpcEndpoint);
            killerClient.open(getRuntimeContext());
        }

        @Override
        public T map(T value) throws Exception {
            killerClient.maybeFail();
            return value;
        }

        @Override
        public void close() throws Exception {
            super.close();
            killerClient.close();
        }
    }
}
