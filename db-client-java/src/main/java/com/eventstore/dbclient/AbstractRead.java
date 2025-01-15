package com.eventstore.dbclient;

import com.eventstore.dbclient.proto.shared.Shared;
import com.eventstore.dbclient.proto.streams.StreamsGrpc;
import com.eventstore.dbclient.proto.streams.StreamsOuterClass;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.CompletableFuture;

abstract class AbstractRead implements Publisher<ReadMessage> {
    protected static final StreamsOuterClass.ReadReq.Options.Builder defaultReadOptions;

    private final GrpcClient client;
    private final OptionsWithBackPressure<?> options;

    protected AbstractRead(GrpcClient client, OptionsWithBackPressure<?> options) {
        this.client = client;
        this.options = options;
    }

    static {
        defaultReadOptions = StreamsOuterClass.ReadReq.Options.newBuilder()
                .setUuidOption(StreamsOuterClass.ReadReq.Options.UUIDOption.newBuilder()
                        .setStructured(Shared.Empty.getDefaultInstance()));
    }

    public abstract StreamsOuterClass.ReadReq.Options.Builder createOptions();

    @Override
    public void subscribe(Subscriber<? super ReadMessage> subscriber) {
        ReadResponseObserver observer = new ReadResponseObserver(options, new ReadStreamConsumer(subscriber));

        this.client.getWorkItemArgs().whenComplete((args, error) -> {
           if (error != null) {
               observer.onError(error);
               return;
           }

            StreamsOuterClass.ReadReq request = StreamsOuterClass.ReadReq.newBuilder()
                    .setOptions(createOptions())
                    .build();

            StreamsGrpc.StreamsStub client = GrpcUtils.configureStub(StreamsGrpc.newStub(args.getChannel()), this.client.getSettings(), this.options);
            observer.onConnected(args);
            subscriber.onSubscribe(observer.getSubscription());
            client.read(request, observer);
        });
    }
}
