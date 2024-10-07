package bigdata.pojo;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import java.io.Serializable;
import java.util.Date;
import java.text.SimpleDateFormat;  

@Table(keyspace = "bigdata", name = "traffic")
public class TrafficData implements Serializable {
    
    @Column(name = "date")
    public String Date;

    @Column(name = "laneId")
    public String LaneId;

    @Column(name = "vehicleCount")
    public Integer VehicleCount;

    public TrafficData() {
        
    }

    public TrafficData(Date date, String laneId, Integer vehicleCount) {
        SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");  
        Date = formatter.format(date); 
        LaneId = laneId;
        VehicleCount = vehicleCount;
    }

    public String getDate() {
        return Date;
    }

    public void setDate(String date) {
        Date = date;
    }

    public String getLaneId() {
        return LaneId;
    }

    public void setLaneId(String laneId) {
        LaneId = laneId;
    }

    public Integer getVehicleCount() {
        return VehicleCount;
    }

    public void setVehicleCount(Integer vehicleCount) {
        VehicleCount = vehicleCount;
    }
}
