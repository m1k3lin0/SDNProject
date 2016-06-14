package net.floodlightcontroller.sdnproject.web;

import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.routing.Router;

import net.floodlightcontroller.restserver.RestletRoutable;

public class SDNProjectRoutable implements RestletRoutable {
	
    /**
     * Create the Restlet router and bind to the proper resources.
     */
	@Override
	public Restlet getRestlet(Context context) {
		Router router = new Router(context);
		router.attach("/request", SDNProjectRequestResource.class );
		router.attach("/add", SDNProjectAddResource.class);
		router.attach("/remove", SDNProjectRemoveResource.class);
		router.attach("/info", SDNProjectInfoResource.class);
		return router;
	}
	
    /**
     * Set the base path for the Topology
     */
	@Override
	public String basePath() {
		return "/wm/SDNProject";
	}

}
