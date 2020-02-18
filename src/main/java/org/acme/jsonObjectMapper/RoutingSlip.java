package org.acme.jsonObjectMapper;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 *
 * @author Magnus
 */
public class RoutingSlip {

    private List<Route> routes = new ArrayList<>();

    public RoutingSlip() {
    }

    public List<Route> getRoutes() {
        return routes;
    }

    public void setRoutes(List<Route> routes) {
        this.routes = routes;
    }

    private void updateRoute(Route route) {
        this.routes = route.getRoutes();
    }

    /**
     * Sorts every route, with the highest priority as the first index. Calls
     * validation on each route, to check if all conditions are true
     *
     * @param metaData The MetaData received from the message
     * @return The highest priority route, where all conditions has been
     * validated true
     * @throws Exception TODO
     */
    public Route validateRoutingSlip(MetaData metaData) throws Exception {
        routes.sort(Comparator.comparing(Route::getPriority).reversed());
        for (Route route : routes) {
            if (route.validateConditionList(metaData)) {
                Route newRoute = route;
                updateRoute(newRoute);
                return newRoute;
            }
        }
        return null;
    }
}
