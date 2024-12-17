package com.eventstore.dbclient;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.Math.min;

class ReadSubscription implements Subscription {
    private final Subscriber<? super ReadMessage> subscriber;
    private ClientCallStreamObserver<?> streamObserver;
    private final AtomicLong requested = new AtomicLong(0);
    private final AtomicLong outstandingRequested = new AtomicLong(0);
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    ReadSubscription(Subscriber<? super ReadMessage> subscriber) {
        this.subscriber = subscriber;
    }

    public void setStreamObserver(ClientCallStreamObserver<?> streamObserver) {
        this.streamObserver = streamObserver;
        streamObserver.disableAutoRequestWithInitial(0);
    }

    public void onError(Throwable error) {
        if (error instanceof StatusRuntimeException) {
            StatusRuntimeException statusRuntimeException = (StatusRuntimeException) error;
            if (statusRuntimeException.getStatus().getCode() == Status.Code.CANCELLED) {
                return;
            }
        }
        if (!terminated.get()) {
            subscriber.onError(error);
        }
        cancel();
    }

    public void onNext(ReadMessage message) {
        if (!terminated.get()) {
            outstandingRequested.decrementAndGet();
            subscriber.onNext(message);
            request0(0);
        }
    }

    public void onCompleted() {
        if (terminated.compareAndSet(false, true)) {
            subscriber.onComplete();
        }
    }

    @Override
    public void request(long n) {
        if (n <= 0) {
            subscriber.onError(new IllegalArgumentException("non-positive subscription request: " + n));
        }

        request0(n);
    }

    private void request0(long n)
    {
        long bufferRequestSize = 512*3/4;
        long currentRequested = requested.addAndGet(n);
        long toRequest = outstandingRequested.get();
        if (currentRequested > 0)
        {
            if (toRequest == 0)
            {
                toRequest = min(currentRequested, 512);
            } else if (toRequest <= bufferRequestSize)
            {
                toRequest = min(currentRequested, 512-toRequest);
            } else {
                return;
            }

            requested.addAndGet(-toRequest);
            outstandingRequested.addAndGet(toRequest);
            streamObserver.request((int)toRequest);
        }
    }

    @Override
    public void cancel() {
        if (terminated.compareAndSet(false, true)) {
            if (streamObserver != null) {
                streamObserver.cancel("Stream has been cancelled manually.", null);
            }
        }
    }

}
