package com.fluxtream.api;

import javax.ws.rs.Path;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 *
 * @author Candide Kemmler (candide@fluxtream.com)
 */
@Path("/events")
@Component("RESTGuestController")
@Scope("request")
public class EventListenerStore {



}
