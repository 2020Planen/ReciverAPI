package org.acme;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.OnOverflow;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.acme.jsonObjectMapper.Message;

@Path("/receiver")
public class ReceiverModule {

    Gson gson = new Gson();

    @Inject
    @Channel("entry")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 300)
    Emitter<String> jsonOutgoing;

    @POST
    @Path("{producerReference}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces(MediaType.APPLICATION_JSON)
    public Response jsonReceiver(@PathParam("producerReference") String producerReference, String content, @Context HttpHeaders headers) {
        JsonParser jsonParser = new JsonParser();
        Message message = new Message("ReciverModule");

        JsonObject json = (JsonObject) jsonParser.parse(content);

        message.validation("address", json.get("address").getAsString());
        message.validation("name", json.get("name").getAsString());
        message.validation("city", json.get("city").getAsString());
        message.validation("phone", json.get("phone").getAsString());
        message.validation("zip", json.get("zip").getAsInt());
        message.setData(json);
        message.setProducerReference(producerReference);

        message.sendToKafkaQue();

        /*
        MultivaluedMap<String, String> rh = headers.getRequestHeaders();
        List<String> contentType = rh.get("Content-Type");
        if (contentType.contains("application/json")) {
            pg.appendJson("producerReference", producerReference);
            pg.appendJson("data", content);
            pg.routingSlipDirector();
            jsonOutgoing.send(pg.getJsonMessageString());
        }
         */
        return Response.ok(gson.toJson(message)).build();
    }

}
