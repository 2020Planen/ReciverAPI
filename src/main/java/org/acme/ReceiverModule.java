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

    @POST
    @Path("{producerReference}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces(MediaType.APPLICATION_JSON)
    public Response jsonReceiver(@PathParam("producerReference") String producerReference, @ValidMessage Message message) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException {
        try {
//            Condition condi = message.validateCondtionSlip();
//            System.out.println(condi.getValue());
        } catch (Exception e) {
            System.out.println("_____________________________________________");
            System.out.println(e);
            //e.printStackTrace();
        }

        message.startLog("ReciverAPI");
        message.setEntryTime(System.currentTimeMillis());
        message.setProducerReference(producerReference);
        message.endLog();
        try {
        message.sendToKafkaQue();
        } catch (Exception e) {
            System.out.println("ss");
        }
//        jsonOutgoing.send(gson.toJson(message));
        return Response.ok(gson.toJson(message)).build();
    }

}
