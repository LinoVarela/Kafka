package models;

public class Result {
    private Route route;
    private Trip trip;

    // Construtor
    public Result(Route route, Trip trip) {
        this.route = route;
        this.trip = trip;
    }

    // Getters e Setters
    public Route getRoute() {
        return route;
    }

    public void setRoute(Route route) {
        this.route = route;
    }

    public Trip getTrip() {
        return trip;
    }

    public void setTrip(Trip trip) {
        this.trip = trip;
    }

    @Override
    public String toString() {
        return "Transacao{" +
                "route=" + route +
                ", trip=" + trip +
                '}';
    }
}
