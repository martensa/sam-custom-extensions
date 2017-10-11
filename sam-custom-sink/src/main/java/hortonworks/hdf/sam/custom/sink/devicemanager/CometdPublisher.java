package hortonworks.hdf.sam.custom.sink.devicemanager;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.ClientTransport;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.common.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

public class CometdPublisher implements CustomProcessorRuntime {
	
	protected static final Logger LOG = LoggerFactory.getLogger(CometdPublisher.class);
	
	private static final String COMETD_PUBSUB_URL = "cometdPubSubUrl";
	private static final String COMETD_PUBSUB_CHANNEL = "cometdPubSubChannel";
	private BayeuxClient bayuexClient;
	
	private String cometdUrl;
	private String cometdChannel;
	
	@Override
	public void initialize(Map<String, Object> config) {
		LOG.info("********** Initializing Cometd Sink processing");
		this.cometdUrl = (String) config.get(COMETD_PUBSUB_URL);
		this.cometdChannel = (String) config.get(COMETD_PUBSUB_CHANNEL);
		
		LOG.info("********** Attempting to connect to Cometd with: " + cometdUrl);
		
		HttpClient httpClient = new HttpClient();
		try {
			httpClient.start();
		} catch (Exception e) {
			e.printStackTrace();
		}

		// Prepare the transport
		Map<String, Object> options = new HashMap<String, Object>();
		ClientTransport transport = new LongPollingTransport(options, httpClient);

		// Create the BayeuxClient
		bayuexClient = new BayeuxClient(cometdUrl, transport);
		
		bayuexClient.handshake();
		boolean handshaken = bayuexClient.waitFor(3000, BayeuxClient.State.CONNECTED);
		if (handshaken)
		{
			LOG.info("********** Connected to Cometd Http PubSub Platform");
		}
		else{
			LOG.info("********** Could not connect to Cometd Http PubSub Platform");
		}

	}

	@Override
	public List<StreamlineEvent> process(StreamlineEvent event) throws ProcessingException {
		Map<String, Object> data = new HashMap<String, Object>();
		for (Entry entry: event.entrySet()){
			data.put(entry.getKey().toString(), entry.getValue());
		}
		//LOG.info("********** CometdPublisher process() Event Fields: " + event.get("fieldsAndValues"));
		LOG.info("********** Publishing Event data to Cometd: " + data);
		bayuexClient.getChannel(cometdChannel).publish(data);
		
		List<StreamlineEvent> events = Collections.<StreamlineEvent>singletonList(event);
		
		return events;
	}

	@Override
	public void validateConfig(Map<String, Object> arg0) throws ConfigException {

	}

	@Override
	public void cleanup() {

	}
}
