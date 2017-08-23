package hortonworks.hdf.sam.custom.processor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

import hortonworks.hdf.sam.custom.processor.schema.TechnicianDestination;
import hortonworks.hdf.sam.custom.processor.schema.TechnicianStatus;

public class RouteTech  implements CustomProcessorRuntime {

	protected static final Logger LOG = LoggerFactory.getLogger(RouteTech.class);
	
	private static final String CONFIG_ZK_HOST = "zkHost";
	private static final String CONFIG_ZK_PORT = "zkPort";
	private static final String CONFIG_ZK_HBASE_PATH = "zkHbasePath";
	private static final String CONFIG_TECH_LOC_TABLE = "techLocationTable";
	private static final String CONFIG_TECH_LOC_TABLE_CF = "techLocationTableCf";
	private static final String CONFIG_DEVICE_TABLE = "deviceTable";
	private static final String CONFIG_DEVICE_TABLE_CF = "deviceTableCf";
	
	private Connection connection;
	private Table techLocationTable = null;
	private Table deviceDetailsTable = null;
	private String zkHost;
	private String zkPort;
	private String zkHbasePath;
	private String techLocationTableName;
	private String techLocationTableCf;
	private String deviceTableName;
	private String deviceTableCf;
	
	public void initialize(Map<String, Object> config) {
		zkHost = (String)config.get(CONFIG_ZK_HOST);
		zkPort = (String)config.get(CONFIG_ZK_PORT);
		zkHbasePath = (String)config.get(CONFIG_ZK_HBASE_PATH);
		techLocationTableName = (String)config.get(CONFIG_TECH_LOC_TABLE);
		techLocationTableCf = (String)config.get(CONFIG_TECH_LOC_TABLE_CF);
		deviceTableName = (String)config.get(CONFIG_DEVICE_TABLE);
		deviceTableCf = (String)config.get(CONFIG_DEVICE_TABLE_CF);
		
		LOG.info("******************** hbase.zookeeper.quorum: " + zkHost);
		LOG.info("******************** hbase.zookeeper.property.clientPort: " + zkPort);
		LOG.info("******************** zookeeper.znode.parent: " + zkHbasePath);
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", zkHost);
		conf.set("hbase.zookeeper.property.clientPort", zkPort);
		conf.set("zookeeper.znode.parent", zkHbasePath);		
		
		try {
			connection = ConnectionFactory.createConnection(conf);
			Admin hbaseAdmin = connection.getAdmin();
		
			while(!hbaseAdmin.tableExists(TableName.valueOf(techLocationTableName))){
				Thread.sleep(1000);
				LOG.info("******************** DeviceMonitor Initialize() Waiting for HBase Tables to be prepared...");
			}
			techLocationTable = connection.getTable(TableName.valueOf(techLocationTableName));
			System.out.println("******************** Acquired HBase Table " + techLocationTable.getName().getNameAsString());
			
			while(!hbaseAdmin.tableExists(TableName.valueOf(deviceTableName))){
				Thread.sleep(1000);
				LOG.info("******************** DeviceMonitor Initialize() Waiting for HBase Tables to be prepared...");
			}
			deviceDetailsTable = connection.getTable(TableName.valueOf(deviceTableName));
			System.out.println("******************** Acquired HBase Table " + deviceDetailsTable.getName().getNameAsString());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
	}

	public List<StreamlineEvent> process(StreamlineEvent event) throws ProcessingException {
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
		//builder.putAll(event);
		
        TechnicianDestination techDestination = null;

		try {
			Get assignedTechGet = new Get(Bytes.toBytes((String)event.get("serial_number")));
			Result assignedTechResult = deviceDetailsTable.get(assignedTechGet);
			String assignedTech = Bytes.toString(assignedTechResult.getColumnLatestCell(Bytes.toBytes(deviceTableCf), Bytes.toBytes("ASSIGNEDTECH")).getValueArray());
			if(assignedTech != null && !assignedTech.equalsIgnoreCase("None")){
				techDestination = nominateTechnician(event);
				if(techDestination.getTechnicianId() != null && techDestination.getStatus().equalsIgnoreCase("Available")){
			        	LOG.info("Emiting Tech Route Request: " + techDestination.getTechnicianId() + " : " + techDestination.toString());
			        	builder.put("technician_id", techDestination.getTechnicianId());
			        	builder.put("status", techDestination.getStatus());
			        	builder.put("longitude", techDestination.getLongitude());
			        	builder.put("latitude", techDestination.getLatitude());
			        	builder.put("event_type", "device-outage");
			        	builder.put("ip_address", techDestination.getIpAddress());
			        	builder.put("port", techDestination.getPort());
			        	builder.put("destination_longitude", techDestination.getDestinationLongitude());
			        	builder.put("destination_latitude", techDestination.getDestinationLatitude());
			    }else{
			        	LOG.info("Technician " + assignedTech + " has already been assigned to this incident");
			    }
			}else{
				LOG.info("Recommended Technician is Null or all Technicians are already assigned");
	        	LOG.info("Need additional Techs in the field.....");
	        	builder.put("technician_id", "null");
	        	builder.put("status", "null");
	        	builder.put("longitude", (Double)event.get("longitude"));
	        	builder.put("latitude", (Double)event.get("latitude"));
	        	builder.put("event_type", "device-outage");
	        	builder.put("ip_address", "null");
	        	builder.put("port", "null");
	        	builder.put("destination_longitude", 0.0);
	        	builder.put("destination_latitude", 0.0);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

        StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
        LOG.info("********** RouteTech process() Output Event: " + enrichedEvent );
        List<StreamlineEvent> newEvents= Collections.<StreamlineEvent>singletonList(enrichedEvent);
        return newEvents; 
	}
	
	public static Double distFrom(Double lat1, Double lng1, Double lat2, Double lng2) {
	       double earthRadius = 6371000; //meters
	       double dLat = Math.toRadians(lat2-lat1);
	       double dLng = Math.toRadians(lng2-lng1);
	       double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
	                  Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
	                  Math.sin(dLng/2) * Math.sin(dLng/2);
	       double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
	       double dist = (Double)(earthRadius * c);

	       return dist;
	}
	    
	@SuppressWarnings("deprecation")
	public TechnicianDestination nominateTechnician (StreamlineEvent event) throws IOException {
	    	TechnicianDestination techDestination = new TechnicianDestination();
	    	TechnicianStatus currentTechStatus = new TechnicianStatus();
	    	TechnicianStatus recommendedTechStatus = new TechnicianStatus();
	    	ResultScanner techLocationScanner;

		    Scan scan = new Scan();
		    scan.addColumn(Bytes.toBytes(techLocationTableCf),Bytes.toBytes("status"));
		    scan.addColumn(Bytes.toBytes(techLocationTableCf),Bytes.toBytes("latitude"));
		    scan.addColumn(Bytes.toBytes(techLocationTableCf),Bytes.toBytes("longitude"));
		    scan.addColumn(Bytes.toBytes(techLocationTableCf),Bytes.toBytes("ip_address"));
		    scan.addColumn(Bytes.toBytes(techLocationTableCf),Bytes.toBytes("port"));
		    
		    ResultScanner scanner = techLocationTable.getScanner(scan);
		    techLocationScanner = scanner;
		    
		    Double currentDistance = null;
	        Double leastDistance = null;        
	        System.out.println("********************** DEVICE ALERT: " + event.get("latitude") + " , " + event.get("longitude"));
	        System.out.println("Recommended Tech: " + recommendedTechStatus.getTechnicianId());
	        System.out.println("Recommended Tech Destination: " + techDestination.getTechnicianId());
	        
	        for(Result result = techLocationScanner.next(); (result != null); result = techLocationScanner.next()) {
	    		currentTechStatus.setTechnicianId(Bytes.toString(result.getRow()));
	    		System.out.println(currentTechStatus.getTechnicianId());
	    		System.out.println("Start of Loop Recommended Tech: " + recommendedTechStatus.getTechnicianId());
	            System.out.println("Start of Loop Recommended Tech Destination: " + techDestination.getTechnicianId());
	            
	            for(KeyValue keyValue : result.list()) {
	    			if(Bytes.toString(keyValue.getQualifier()).equalsIgnoreCase("status")){
	    				System.out.println("Qualifier : " + Bytes.toString(keyValue.getQualifier()) + " : Value : " + Bytes.toString(keyValue.getValue()));
	    				currentTechStatus.setStatus(Bytes.toString(keyValue.getValue()));
	    			} else if(Bytes.toString(keyValue.getQualifier()).equalsIgnoreCase("latitude")){
	    				System.out.println("Qualifier : " + Bytes.toString(keyValue.getQualifier()) + " : Value : " + Bytes.toDouble(keyValue.getValue()));
	    				currentTechStatus.setLatitude(Bytes.toDouble(keyValue.getValue()));
	    			} else if(Bytes.toString(keyValue.getQualifier()).equalsIgnoreCase("longitude")){
	    				System.out.println("Qualifier : " + Bytes.toString(keyValue.getQualifier()) + " : Value : " + Bytes.toDouble(keyValue.getValue()));
	    				currentTechStatus.setLongitude(Bytes.toDouble(keyValue.getValue()));
	    			} else if(Bytes.toString(keyValue.getQualifier()).equalsIgnoreCase("ip_address")){
	    				System.out.println("Qualifier : " + Bytes.toString(keyValue.getQualifier()) + " : Value : " + Bytes.toString(keyValue.getValue()));
	    				currentTechStatus.setIpAddress(Bytes.toString(keyValue.getValue()));
	    			} else if(Bytes.toString(keyValue.getQualifier()).equalsIgnoreCase("port")){
	    				System.out.println("Qualifier : " + Bytes.toString(keyValue.getQualifier()) + " : Value : " + Bytes.toString(keyValue.getValue()));
	    				currentTechStatus.setPort(Bytes.toString(keyValue.getValue()));
	    			}
	    		}
	    		System.out.println("Post Lookup Loop Recommended Tech: " + recommendedTechStatus.getTechnicianId());
	            System.out.println("Post Lookup Loop Recommended Tech Destination: " + techDestination.getTechnicianId());
	    		System.out.println("Current Tech Id: " + currentTechStatus.getTechnicianId());
	    		System.out.println("Current Tech Status: " + currentTechStatus.getStatus());
	    		System.out.println("Current Tech Latitude: " + currentTechStatus.getLatitude());
	    		System.out.println("Current Tech Longitude: " + currentTechStatus.getLongitude());
	    		System.out.println("Current IpAddress: " + currentTechStatus.getIpAddress());
	    		System.out.println("Current Port: " + currentTechStatus.getPort());
	    		
	    		System.out.println("Inside Loop Recommended Tech: " + recommendedTechStatus.getTechnicianId());
	            System.out.println("Inside Loop Recommended Tech Destination: " + techDestination.getTechnicianId());
	    		
	            currentDistance = distFrom(currentTechStatus.getLatitude(), currentTechStatus.getLongitude(), Double.valueOf((String)event.get("latitude")), Double.valueOf((String)event.get("longitude").toString()));
	            if(currentTechStatus.getStatus().equalsIgnoreCase("Available")){
	            	System.out.println("Current Tech is available for repair");
	            	if(currentDistance == null || leastDistance == null){
	            		System.out.println("Current Tech: " + currentTechStatus.getTechnicianId() + " Current Distance: " + currentDistance + " Least Distance:" + leastDistance);
	            		System.out.println("Setting current tech as recommended tech since least distance is null");
	            		recommendedTechStatus = copyTechnicianStatus(currentTechStatus);
	            		leastDistance = currentDistance;
	            	}
	            	else if(currentDistance < leastDistance){
	            		System.out.println("Current Tech: " + currentTechStatus.getTechnicianId() + " Current Distance: " + currentDistance + " Least Distance:" + leastDistance);
	            		System.out.println("Setting current tech as recommended tech since they are currently closest to incident");
	            		recommendedTechStatus = copyTechnicianStatus(currentTechStatus);
	            		leastDistance = currentDistance;   
	            	}
	            	else{
	            		System.out.println("Current Tech: " + currentTechStatus.getTechnicianId() + " Current Distance: " + currentDistance + " Least Distance:" + leastDistance);
	            		System.out.println("Current tech is not recommended as some other tech is closer to the incident");
	            	}
	            }
	            else{
	            	System.out.println("Current Tech: " + currentTechStatus.getTechnicianId() + " Current Distance: " + currentDistance + " Least Distance:" + leastDistance);
	                System.out.println("Technician " + currentTechStatus.getTechnicianId() + " is already assigned to a repair");
	            }
	            System.out.println("End of Inside Loop Recommended Tech: " + recommendedTechStatus.getTechnicianId());
	        }
	    	
		    System.out.println("Technician " + recommendedTechStatus.getTechnicianId() + " is recommended for this repair");
	        
		    Put assignTechPut = new Put(Bytes.toBytes((String)event.get("serial_number")));
		    assignTechPut.addColumn(Bytes.toBytes(deviceTableCf), Bytes.toBytes("ASSIGNEDTECH"), Bytes.toBytes(recommendedTechStatus.getTechnicianId()));
		    deviceDetailsTable.put(assignTechPut);
		    
	        techDestination.setTechnicianId(recommendedTechStatus.getTechnicianId());
	        techDestination.setLatitude(recommendedTechStatus.getLatitude());
	        techDestination.setLongitude(recommendedTechStatus.getLongitude());
	        techDestination.setDestinationLatitude(Double.valueOf((String)event.get("latitude")));
	        techDestination.setDestinationLongitude(Double.valueOf((String)event.get("longitude")));
	        techDestination.setStatus(recommendedTechStatus.getStatus());
	        techDestination.setIpAddress(recommendedTechStatus.getIpAddress());
	        techDestination.setPort(recommendedTechStatus.getPort());
	        
	        System.out.println("Recommended Tech Destination: " + techDestination.getTechnicianId() + " : " + techDestination.getDestinationLatitude() + " : " + techDestination.getDestinationLongitude());
	        
	        return techDestination;
	    }
	    
	    public TechnicianStatus copyTechnicianStatus(TechnicianStatus sourceTechStatus){
	    	TechnicianStatus targetTechStatus = new TechnicianStatus();
	    	targetTechStatus.setTechnicianId(sourceTechStatus.getTechnicianId());
	    	targetTechStatus.setIpAddress(sourceTechStatus.getIpAddress());
	    	targetTechStatus.setPort(sourceTechStatus.getPort());
	    	targetTechStatus.setLatitude(sourceTechStatus.getLatitude());
	    	targetTechStatus.setLongitude(sourceTechStatus.getLongitude());
	    	targetTechStatus.setStatus(sourceTechStatus.getStatus());
	    	return targetTechStatus ;
	    }
	    
	    public void validateConfig(Map<String, Object> arg0) throws ConfigException {
			
		}
	    
		public void cleanup() {

		}
}
