package org.acme;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.smallrye.reactive.messaging.annotations.Emitter;
import io.smallrye.reactive.messaging.annotations.Channel;
import io.smallrye.reactive.messaging.annotations.OnOverflow;
import java.util.HashMap;
import java.util.Map;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.json.JSONObject;
import org.json.XML;

@Path("/receiver")
public class ExampleResource {
    Gson gson = new Gson();
    
    @Inject 
    @Channel("entry") 
    @OnOverflow(value = OnOverflow.Strategy.BUFFER, bufferSize = 300) Emitter<String> jsonOutgoing;
    
    @POST
    @Path("/json")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response jsonReceiver(String jsonContent){
        addTimestamp(jsonContent);
        return Response.ok().build();
    }

    @POST
    @Path("/xml")
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_JSON)
    public Response xmlReceiver(String xmlContent){
        JSONObject xmlJsonObject = XML.toJSONObject(xmlContent);
        addTimestamp(xmlJsonObject.toString());
        return Response.ok(xmlJsonObject.toString()).build();
    }
    
    public void addTimestamp(String jsonString){
        TimeStamp ts = new TimeStamp("ReceiverAPIModule");
        Map map = gson.fromJson(jsonString, Map.class);
        map.put("Timestamp", ts.toString());
        java.lang.reflect.Type gsonType = new TypeToken<HashMap>(){}.getType();
        String jsonTimestamped = gson.toJson(map,gsonType);
        publishJson(jsonTimestamped);
    }
    
    public void publishJson(String jsonTimestamped){
        jsonOutgoing.send(jsonTimestamped);
    }
}