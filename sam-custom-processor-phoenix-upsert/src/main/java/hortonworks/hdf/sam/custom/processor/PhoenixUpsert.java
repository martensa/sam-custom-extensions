package hortonworks.hdf.sam.custom.processor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hortonworks.streamline.streams.StreamlineEvent;
import com.hortonworks.streamline.streams.common.StreamlineEventImpl;
import com.hortonworks.streamline.streams.exception.ConfigException;
import com.hortonworks.streamline.streams.exception.ProcessingException;
import com.hortonworks.streamline.streams.runtime.CustomProcessorRuntime;

/**
 * A Phoenix upsert processor. The processor will execute a 
 * user provided UPSERT statement. 
 * UPSERT statements can refer to the values from the input schema using the placeholder ${field}
 * 
 *
 */
public class PhoenixUpsert implements CustomProcessorRuntime {
	protected static final Logger LOG = LoggerFactory.getLogger(PhoenixUpsert.class);

	static final String CONFIG_ZK_SERVER_URL = "zkServerUrl";
	static final String CONFIG_UPSERT_SQL = "upsertSQL";
	static final String CONFIG_SECURE_CLUSTER = "secureCluster";
	static final String CONFIG_KERBEROS_CLIENT_PRINCIPAL = "kerberosClientPrincipal";
	static final String CONFIG_KERBEROS_KEYTAB_FILE = "kerberosKeyTabFile";

	private Connection phoenixConnection = null;
	private String upsertSQLStatement = null;
	private boolean secureCluster;
	
	public void cleanup() {
		DbUtils.closeQuietly(phoenixConnection);
	}

	public void initialize(Map<String, Object> config) {
		LOG.info("Initializing + " + PhoenixUpsert.class.getName());

		this.upsertSQLStatement =  ((String) config.get(CONFIG_UPSERT_SQL)).trim();
		LOG.info("The configured SQL is: " + upsertSQLStatement);
		LOG.info("Seure Cluster Flag is: " + config.get(CONFIG_SECURE_CLUSTER));
		LOG.info("Kerberos Principal is: " + config.get(CONFIG_KERBEROS_CLIENT_PRINCIPAL));
		LOG.info("Kerberos KeyTab is: " + config.get(CONFIG_KERBEROS_KEYTAB_FILE));		
		
		setUpJDBCPhoenixConnection(config);		
	}

	public List<StreamlineEvent> process(StreamlineEvent event) throws ProcessingException {
		LOG.info("********** PhoenixUpsert process() Event[" + event + "] ");
		upsert(event);
		StreamlineEventImpl.Builder builder = StreamlineEventImpl.builder();
		builder.putAll(event);
		List<StreamlineEvent> newEvents = Collections.<StreamlineEvent> singletonList(event);

		return newEvents;
	}

	private void upsert(StreamlineEvent event) {
		StrSubstitutor strSub = new StrSubstitutor(event);

		String upsertSQLToExecute = strSub.replace(this.upsertSQLStatement);
		Statement statement = null;
		try {
			LOG.info("********** PhoenixUpsert upsert() SQL with substitued fields to be executed is: "+ upsertSQLToExecute);
			statement = phoenixConnection.createStatement();
			statement.executeUpdate(upsertSQLToExecute);
			phoenixConnection.commit();

		} catch (SQLException e) {
			String errorMsg = "********** PhoenixUpsert upsert() Error upserting with upsert sql[" + upsertSQLToExecute + "]";
			LOG.error(errorMsg, e);
			throw new RuntimeException(errorMsg, e);

		} finally {
			DbUtils.closeQuietly(statement);
		}
	}

	@Override
	public void validateConfig(Map<String, Object> config) throws ConfigException {
		boolean isSecureCluster =  config.get(CONFIG_SECURE_CLUSTER) != null && ((Boolean) config.get(CONFIG_SECURE_CLUSTER)).booleanValue();
		if(isSecureCluster) {
			String principal = (String) config.get(CONFIG_KERBEROS_CLIENT_PRINCIPAL);
			String keyTab  = (String) config.get(CONFIG_KERBEROS_KEYTAB_FILE);
			if(StringUtils.isEmpty(principal) || StringUtils.isEmpty(keyTab)) {
				throw new ConfigException("If Secure Cluster, Kerberos principal and key tabe must be provided");
			}
		}
	}

	private String constructInSecureJDBCPhoenixConnectionUrl(String zkServerUrl) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("jdbc:phoenix:").append(zkServerUrl).append(":/hbase-unsecure");
		return buffer.toString();
	}
	
	//jdbc:phoenix:zk_quorum:2181:/hbase-secure:hbase@EXAMPLE.COM:/hbase-secure/keytab/keytab_file
	private String constructSecureJDBCPhoenixConnectionUrl(String zkServerUrl, String clientPrincipal, String keyTabFile) {
		StringBuffer buffer = new StringBuffer();
		buffer.append("jdbc:phoenix:").append(zkServerUrl).append(":/hbase-secure").append(":").append(clientPrincipal).append(":").append(keyTabFile);
		return buffer.toString();
	}	
		
	private void setUpJDBCPhoenixConnection(Map<String, Object> config) {
		String zkServerUrl = (String) config.get(CONFIG_ZK_SERVER_URL);
		boolean secureCluster = config.get(CONFIG_SECURE_CLUSTER) != null && ((Boolean)config.get(CONFIG_SECURE_CLUSTER)).booleanValue();
		String clientPrincipal = (String)config.get(CONFIG_KERBEROS_CLIENT_PRINCIPAL);
		String keyTabFile =  (String)config.get(CONFIG_KERBEROS_KEYTAB_FILE);		
		
		String jdbcPhoenixConnectionUrl = "";
		if(secureCluster) {
			jdbcPhoenixConnectionUrl = constructSecureJDBCPhoenixConnectionUrl(zkServerUrl, clientPrincipal, keyTabFile);
		} else {
			jdbcPhoenixConnectionUrl = constructInSecureJDBCPhoenixConnectionUrl(zkServerUrl);
		}
		
		LOG.info("Initializing Phoenix Connection with JDBC connection string["+ jdbcPhoenixConnectionUrl + "]");
		try {
			phoenixConnection = DriverManager.getConnection(jdbcPhoenixConnectionUrl);
		} catch (SQLException e) {
			String error = "Error creating Phoenix JDBC connection";
			LOG.error(error, e);
			throw new RuntimeException(error);
		}
		LOG.info("Successfully created Phoenix Connection with JDBC connection string["+ jdbcPhoenixConnectionUrl + "]");
	}	
}
