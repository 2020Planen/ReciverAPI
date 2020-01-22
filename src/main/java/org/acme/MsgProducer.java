package org.acme;
/*
import com.google.gson.Gson;
import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.OnOverflow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Outgoing;

@ApplicationScoped
public class MsgProducer {

    Gson gson = new Gson();
    private AtomicInteger otc = new AtomicInteger();

    @Outgoing("entry")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 300)
    public Flowable<String> generate() {
        try {
            for (int i = 0; i < 100; i++) {
                
            
                return Flowable.interval(1, TimeUnit.MILLISECONDS)
                        .map(tick -> "{\"id\":" + Integer.toString(i) + "}");
            }
        } catch (Exception e) {
            return Flowable.empty();
        }
        return null;
    }
}
*/
