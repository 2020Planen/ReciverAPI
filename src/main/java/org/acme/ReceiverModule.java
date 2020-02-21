package org.acme;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.OnOverflow;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.acme.jsonObjectMapper.Message;
import org.acme.validate.validateMessage.ValidMessage;

/**
 *
 * @author Magnus
 */
@Path("/receiver")
public class ReceiverModule {

    private Gson gson = new Gson();
    ObjectMapper objectMapper = new ObjectMapper();
    Sizeof sizeOf = new Sizeof();
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
    public Response jsonReceiver(@PathParam("producerReference") String producerReference, @ValidMessage(moduleName = "ReciverAPI") Message message) throws Exception {
        message.setEntryTime(System.currentTimeMillis());
        message.setProducerReference(producerReference);
        message.endLog();

        jsonOutgoing.send(gson.toJson(message));
        return Response.ok(gson.toJson(message)).build();
    }

    @Inject
    @Channel("entry_bulk")
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 300)
    Emitter<byte[]> jsonBulkOutgoing;

    @POST
    @Path("bulk/{producerReference}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces(MediaType.APPLICATION_JSON + ";charset=utf-8")
    public Response jsonBulkReceiver(@PathParam("producerReference") String producerReference, List<Message> messageList) throws ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, JsonProcessingException, Exception {
        messageList.forEach(message -> {
            message.startLog("ReciverAPI");
            message.setEntryTime(System.currentTimeMillis());
            message.setProducerReference(producerReference);
            message.endLog();
        });
        System.out.println("æøå");
        System.out.println(messageList.get(0).getMetaData().getAddress());
        String test = objectMapper.writeValueAsString(messageList);
        final byte[] utf8Bytes = test.getBytes("UTF-8");
        System.out.println(utf8Bytes.length + " ___Size bytes");
        jsonBulkOutgoing.send(compress(test));
        return Response.ok("ok").build();
    }

    public static byte[] compress(String str) throws IOException {
        if ((str == null) || (str.length() == 0)) {
            return null;
        }
        ByteArrayOutputStream obj = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(obj);
        gzip.write(str.getBytes("UTF-8"));
        gzip.flush();
        gzip.close();
        return obj.toByteArray();
    }
}
