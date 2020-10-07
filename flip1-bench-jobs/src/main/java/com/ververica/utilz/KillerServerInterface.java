package com.ververica.utilz;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public interface KillerServerInterface extends RpcGateway {
    CompletableFuture<Action> register(UUID client, @RpcTimeout Time timeout);
    CompletableFuture<Acknowledge> unregister(UUID client, @RpcTimeout Time timeout);

    enum Action {
        KILL,
        IGNORE
    }
}
