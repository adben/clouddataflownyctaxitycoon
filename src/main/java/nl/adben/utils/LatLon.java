package nl.adben.utils;

import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;

/**
 * Better than java serialization for the counting case
 */
@DefaultCoder(AvroCoder.class)
public class LatLon {
    public LatLon() {}

    public LatLon(double lat, double lon) {
        this.lat = lat;
        this.lon = lon;
    }

    public double lat;
    public double lon;
}
