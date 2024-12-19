package models;

public class Trip {
    private String tripId;
    private String routeId;
    private String origin;
    private String destination;
    private String passengerName;
    private String transportType;

    public Trip() {}


    public Trip(String tripId, String routeId, String origin, String destination,
                String passengerName, String transportType) {
        this.tripId = tripId;
        this.routeId = routeId;
        this.origin = origin;
        this.destination = destination;
        this.passengerName = passengerName;
        this.transportType = transportType;
    }

    // Getters e Setters
    public String getTripId() {
        return tripId;
    }

    public void setTripId(String tripId) {
        this.tripId = tripId;
    }

    public String getRouteId() {
        return routeId;
    }

    public void setRouteId(String routeId) {
        this.routeId = routeId;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getPassengerName() {
        return passengerName;
    }

    public void setPassengerName(String passengerName) {
        this.passengerName = passengerName;
    }

    public String getTransportType() {
        return transportType;
    }

    public void setTransportType(String transportType) {
        this.transportType = transportType;
    }

    @Override
    public String toString() {
        return "Trip{" +
                "tripId='" + tripId + '\'' +
                ", routeId='" + routeId + '\'' +
                ", origin='" + origin + '\'' +
                ", destination='" + destination + '\'' +
                ", passengerName='" + passengerName + '\'' +
                ", transportType='" + transportType + '\'' +
                '}';
    }
}
