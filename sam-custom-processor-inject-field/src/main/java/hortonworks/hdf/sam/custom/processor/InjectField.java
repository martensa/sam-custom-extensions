package hortonworks.hdf.sam.custom.processor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

public class InjectField  implements CustomProcessorRuntime {

	protected static final Logger LOG = LoggerFactory.getLogger(InjectField.class);
	
	private static final String NEW_FIELD_NAME = "newFieldName";
	private static final String NEW_FIELD_VALUE = "newFieldValue";
	private String newFieldName;
	private String newFieldValue;
	
	public void initialize(Map<String, Object> config) {
		newFieldName = (String) config.get(NEW_FIELD_NAME);
		newFieldValue = (String) config.get(NEW_FIELD_VALUE);
	}

	public List<StreamlineEvent> process(StreamlineEvent event) throws ProcessingException {
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
		builder.putAll(event);
		builder.put(newFieldName,newFieldValue);
        StreamlineEvent enrichedEvent = builder.dataSourceId(event.getDataSourceId()).build();
        LOG.info("********** InjectField process() Output Event: " + enrichedEvent );
        List<StreamlineEvent> newEvents = Collections.<StreamlineEvent>singletonList(enrichedEvent);
        return newEvents;  
	}

	public void validateConfig(Map<String, Object> arg0) throws ConfigException {
		
	}

	@Override
	public void cleanup() {
		
	}
}
