package org.acme;

import com.google.gson.Gson;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.OnOverflow;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.acme.jsonObjectMapper.Message;

@Path("/receiver")
public class ReceiverModule {

    private Gson gson = new Gson();

    @Inject
    @Channel("entry")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 300)
    Emitter<String> jsonOutgoing;

    @POST
    @Path("{producerReference}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces(MediaType.APPLICATION_JSON)
    public Response jsonReceiver(@PathParam("producerReference") String producerReference, Message message) {
        message.startLog("ReciverAPI");
        message.setProducerReference(producerReference);
        message.endLog();
        jsonOutgoing.send(gson.toJson(message));

        return Response.ok(gson.toJson(message)).build();
    }

}
