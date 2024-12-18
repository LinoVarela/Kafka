package models;


public class Route {
    private String routeId;
    private String origin;
    private String destination;
    private int passengerCapacity;
    private String transportType;
    private String operator;
    private int passengerCount; 

    public int getPassengerCount() {
        return passengerCount;
    }

    public void setPassengerCount(int passengerCount) {
        this.passengerCount = passengerCount;
    }

    public Route(String routeId, String origin, String destination, int passengerCapacity, String transportType, String operator) {
        this.routeId = routeId;
        this.origin = origin;
        this.destination = destination;
        this.passengerCapacity = passengerCapacity;
        this.transportType = transportType;
        this.operator = operator;
    }

    // Getters and Setters
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

    public int getPassengerCapacity() {
        return passengerCapacity;
    }

    public void setPassengerCapacity(int passengerCapacity) {
        this.passengerCapacity = passengerCapacity;
    }

    public String getTransportType() {
        return transportType;
    }

    public void setTransportType(String transportType) {
        this.transportType = transportType;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
        this.operator = operator;
    }

    @Override
    public String toString() {
        return "Route{" +
                "routeId='" + routeId + '\'' +
                ", origin='" + origin + '\'' +
                ", destination='" + destination + '\'' +
                ", passengerCapacity=" + passengerCapacity +
                ", transportType='" + transportType + '\'' +
                ", operator='" + operator + '\'' +
                '}';
    }
}
