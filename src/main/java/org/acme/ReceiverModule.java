package org.acme;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.OnOverflow;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.json.XML;

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
        ConfigJsonObject dir = new ConfigJsonObject("ReceiverAPIModule");

        MultivaluedMap<String, String> rh = headers.getRequestHeaders();
        List<String> contentType = rh.get("Content-Type");
        try {
            if (contentType.contains("application/json")) {
                publishJson(dir, producerReference, content);
            } else {
                publishJson(dir, producerReference, XML.toJSONObject(content).toString());
            }
        } catch (Exception e) {
            System.out.println(e);
        }

        return Response.ok("Succes").build();
    }

    public void publishJson(ConfigJsonObject dir, String producerReference, String content) throws IOException {
        dir.convertJsonToEntity(content);
        dir.addJsonObject("producerReference", producerReference);
        jsonOutgoing.send(dir.getJsonObjectString());
    }
}
