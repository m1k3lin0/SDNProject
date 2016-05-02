package net.floodlightcontroller.sdnproject;

import org.restlet.Context;
import org.restlet.Restlet;
import org.restlet.routing.Router;

import net.floodlightcontroller.restserver.RestletRoutable;

public class SDNProjectRoutable implements RestletRoutable {

	@Override
	public Restlet getRestlet(Context context) {
		Router router = new Router(context);
		router.attach("/request", SDNProjectRequestResource.class );
		router.attach("/add", SDNProjectAddResource.class);
		router.attach("/remove", SDNProjectRemoveResource.class);
		router.attach("/info", SDNProjectInfoResource.class);
		return router;
	}

	@Override
	public String basePath() {
		return "/wm/SDNProject";
	}

}
