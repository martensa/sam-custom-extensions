package hortonworks.hdf.sam.custom.processor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

public class ExplodeCollection  implements CustomProcessorRuntime {

	protected static final Logger LOG = LoggerFactory.getLogger(ExplodeCollection.class);
	
	private static final String ARRAY_FIELD_KEY = "arrayField";
	private String arrayFieldKey;
	
	public void initialize(Map<String, Object> config) {
		arrayFieldKey = (String) config.get(ARRAY_FIELD_KEY);
	}

	public List<StreamlineEvent> process(StreamlineEvent event) throws ProcessingException {
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
        builder.putAll(event);
        
        Map<String, Object> arrayFields = new HashMap<String, Object>();
        
        List<Object> targetArray = (List<Object>) event.get(arrayFieldKey);
        
        int count = 0;
        Iterator<Object> iterator = targetArray.iterator();
        while(iterator.hasNext()){
        	arrayFields.put("field_"+count, iterator.next());
        	count++;
        }
        
        builder.putAll(arrayFields);
		
        StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
        LOG.info("********** ExplodeCollection process() Output Event: " + enrichedEvent );
        List<StreamlineEvent> newEvents= Collections.<StreamlineEvent>singletonList(enrichedEvent);
        return newEvents;  
	}

	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		
	}

	@Override
	public void cleanup() {
		
	}
}
