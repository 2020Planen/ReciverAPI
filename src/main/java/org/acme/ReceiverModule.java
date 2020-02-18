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
import org.acme.jsonObjectMapper.Condition;
import org.acme.jsonObjectMapper.Message;
import org.acme.validate.validateMessage.ValidMessage;

/**
 *
 * @author Magnus
 */
@Path("/receiver")
public class ReceiverModule {

    private Gson gson = new Gson();

    @Inject
    @Channel("entry")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 300)
    Emitter<String> jsonOutgoing;

    /**
     * Receives any given json object. Deserialize the object, into
     * Message.class. Starts the log, and setting the producer reference.
     * Serialize the Message.class back to json, and sending it to the entry que
     * for further handling.
     *
     * @param producerReference Which post service has been used
     * @param message Deserialize the json Object, and maps the json into the
     * Message.class
     * @return Response code(200), and returns the jsonify Message.class
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     */
    @POST
    @Path("{producerReference}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces(MediaType.APPLICATION_JSON)
    public Response jsonReceiver(@PathParam("producerReference") String producerReference, @ValidMessage Message message) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        message.startLog("ReciverAPI");
        message.setEntryTime(System.currentTimeMillis());
        message.setProducerReference(producerReference);
        message.endLog();


        jsonOutgoing.send(gson.toJson(message)); 
        return Response.ok(gson.toJson(message)).build();
    }

}
