package hortonworks.hdf.sam.custom.processor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.common.exception.ConfigException;
import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

public class ExplodeCollection  implements CustomProcessorRuntime {

	protected static final Logger LOG = LoggerFactory.getLogger(ExplodeCollection.class);
	
	private static final String EXPECTED_ARRAY_LENGTH = "expectedArrayLength";
	private static final String ARRAY_GROUPBY_FIELD_KEY = "arrayGroupByField";
	private int expectedArrayLength = 0;
	private String arrayGroupByFieldKey;
	private Map<Object,Object> buffer;
	
	public void initialize(Map<String, Object> config) {
		expectedArrayLength = (Integer) config.get(EXPECTED_ARRAY_LENGTH);
		arrayGroupByFieldKey = (String) config.get(ARRAY_GROUPBY_FIELD_KEY);
		buffer = new HashMap<Object,Object>();
	}

	public List<StreamlineEvent> process(StreamlineEvent event) throws ProcessingException {
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
        builder.putAll(event);
        
        Map<String, Object> arrayFields = new HashMap<String, Object>();
        
        Object arrayGroupKey = event.get(arrayGroupByFieldKey);
        List<Object> incomingArray = (List<Object>) event.get("arrayField");
        List<Object> storedPartialArray = new ArrayList<Object>();
        List<Object> completedArray = new ArrayList<Object>();
    	
        int remainingArraySlots = 0;
        if(buffer.containsKey(arrayGroupKey)){
        	storedPartialArray = (List<Object>) buffer.get(arrayGroupKey);
        	remainingArraySlots = expectedArrayLength - storedPartialArray.size();
        }else{
        	remainingArraySlots = expectedArrayLength;
        }
        
        if(incomingArray.size() < remainingArraySlots){
       		storedPartialArray.addAll(incomingArray);
       		buffer.put(arrayGroupKey, storedPartialArray);
       	}else if(incomingArray.size() > remainingArraySlots){
       		storedPartialArray.add(incomingArray.subList(0, remainingArraySlots-1));
       		completedArray.addAll(storedPartialArray);
       		List<Object> remainderPartialArray = incomingArray.subList(remainingArraySlots,incomingArray.size()-1);
       		buffer.put(arrayGroupKey, remainderPartialArray);
       	}else if(incomingArray.size() == remainingArraySlots){
       		storedPartialArray.addAll(incomingArray);
       		completedArray.addAll(storedPartialArray);
        	buffer.remove(arrayGroupKey);
        }

        if(completedArray.size() == expectedArrayLength){
        	int count = 0;
        	Iterator<Object> iterator = completedArray.iterator();
        	while(iterator.hasNext()){
        		arrayFields.put("field_"+count, iterator.next());
        		count++;
        	}

        	builder.putAll(arrayFields);
		
        	StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
        	LOG.info("********** ExplodeCollection process() Output Event: " + enrichedEvent );
        	List<StreamlineEvent> newEvents = Collections.<StreamlineEvent>singletonList(enrichedEvent);
        	return newEvents;
        }else{
        	LOG.info("********** ExplodeCollection process() Expected Array size for Key: "+ arrayGroupKey +" : has not yet been reached... "
        			+ "Collected: " + storedPartialArray.size() + "... Expected: " + expectedArrayLength);
        	return null;
        }
	}

	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		
	}

	public void cleanup() {
		
	}
}
