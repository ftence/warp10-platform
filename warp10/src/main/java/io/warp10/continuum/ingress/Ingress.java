//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.continuum.ingress;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.warp10.SSLUtils;
import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.JettyUtil;
import io.warp10.continuum.KafkaProducerPool;
import io.warp10.continuum.KafkaSynchronizedConsumerPool;
import io.warp10.continuum.KafkaSynchronizedConsumerPool.ConsumerFactory;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.egress.CORSHandler;
import io.warp10.continuum.egress.ThriftDirectoryClient;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.DirectoryClient;
import io.warp10.continuum.store.thrift.data.KafkaDataMessage;
import io.warp10.continuum.store.thrift.data.KafkaDataMessageType;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.SipHashInline;
import io.warp10.sensision.Sensision;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.util.BlockingArrayQueue;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

// FIXME(hbs): handle archive

/**
 * This is the class which ingests metrics.
 */
public class Ingress implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(Ingress.class);

  static final Long NO_LAST_ACTIVITY = Long.MIN_VALUE;

  /**
   * Set of required parameters, those MUST be set
   */
  private static final String[] REQUIRED_PROPERTIES = new String[]{
      Configuration.INGRESS_HOST,
      Configuration.INGRESS_PORT,
      Configuration.INGRESS_ACCEPTORS,
      Configuration.INGRESS_SELECTORS,
      Configuration.INGRESS_IDLE_TIMEOUT,
      Configuration.INGRESS_JETTY_THREADPOOL,
      Configuration.INGRESS_ZK_QUORUM,
      Configuration.INGRESS_KAFKA_META_BROKERLIST,
      Configuration.INGRESS_KAFKA_META_TOPIC,
      Configuration.INGRESS_KAFKA_DATA_BROKERLIST,
      Configuration.INGRESS_KAFKA_DATA_TOPIC,
      Configuration.INGRESS_KAFKA_DATA_POOLSIZE,
      Configuration.INGRESS_KAFKA_METADATA_POOLSIZE,
      Configuration.INGRESS_KAFKA_DATA_MAXSIZE,
      Configuration.INGRESS_KAFKA_METADATA_MAXSIZE,
      Configuration.INGRESS_VALUE_MAXSIZE,
      Configuration.INGRESS_DELETE_SHUFFLE,
      Configuration.DIRECTORY_PSK,
  };

  final String metaTopic;

  final String dataTopic;

  final DirectoryClient directoryClient;

  final long maxValueSize;

  final long ttl;

  final boolean useDatapointTs;

  final String cacheDumpPath;

  DateTimeFormatter fmt = ISODateTimeFormat.dateTimeParser();

  /**
   * List of pending Kafka messages containing metadata (one per Thread)
   */
  final ThreadLocal<List<KeyedMessage<byte[], byte[]>>> metadataMessages = new ThreadLocal<List<KeyedMessage<byte[], byte[]>>>() {
    protected java.util.List<kafka.producer.KeyedMessage<byte[], byte[]>> initialValue() {
      return new ArrayList<KeyedMessage<byte[], byte[]>>();
    }

    ;
  };

  /**
   * Byte size of metadataMessages
   */
  ThreadLocal<AtomicLong> metadataMessagesSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong();
    }

    ;
  };

  /**
   * Size threshold after which we flush metadataMessages into Kafka
   */
  final long METADATA_MESSAGES_THRESHOLD;

  /**
   * List of pending Kafka messages containing data
   */
  final ThreadLocal<List<KeyedMessage<byte[], byte[]>>> dataMessages = new ThreadLocal<List<KeyedMessage<byte[], byte[]>>>() {
    protected java.util.List<kafka.producer.KeyedMessage<byte[], byte[]>> initialValue() {
      return new ArrayList<KeyedMessage<byte[], byte[]>>();
    }

    ;
  };

  /**
   * Byte size of dataMessages
   */
  ThreadLocal<AtomicLong> dataMessagesSize = new ThreadLocal<AtomicLong>() {
    protected AtomicLong initialValue() {
      return new AtomicLong();
    }

    ;
  };

  /**
   * Size threshold after which we flush dataMessages into Kafka
   * <p>
   * This and METADATA_MESSAGES_THRESHOLD has to be lower than the configured Kafka max message size (message.max.bytes)
   */
  final long DATA_MESSAGES_THRESHOLD;

  /**
   * Pool of producers for the 'data' topic
   */
  final Producer<byte[], byte[]>[] dataProducers;

  int dataProducersCurrentPoolSize = 0;

  /**
   * Pool of producers for the 'metadata' topic
   */
  final KafkaProducerPool metaProducerPool;

  /**
   * Number of classId/labelsId to remember (to avoid pushing their metadata to Kafka)
   * Memory footprint is that of a BigInteger whose byte representation is 16 bytes, so probably
   * around 40 bytes
   * FIXME(hbs): need to compute exactly
   */
  private int METADATA_CACHE_SIZE = 10000000;

  /**
   * Cache used to determine if we should push metadata into Kafka or if it was previously seen.
   * Key is a BigInteger constructed from a byte array of classId+labelsId (we cannot use byte[] as map key)
   */
  final Map<BigInteger, Long> metadataCache = new LinkedHashMap<BigInteger, Long>(100, 0.75F, true) {
    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<BigInteger, Long> eldest) {
      return this.size() > METADATA_CACHE_SIZE;
    }
  };

  final Properties properties;

  final long[] classKey;
  final long[] labelsKey;

  final byte[] AES_KAFKA_META;
  final long[] SIPHASH_KAFKA_META;

  final byte[] aesDataKey;
  final long[] siphashDataKey;

  final boolean sendMetadataOnDelete;
  final boolean sendMetadataOnStore;

  private final KafkaSynchronizedConsumerPool pool;

  final boolean doShuffle;

  final boolean rejectDelete;

  final boolean activityTracking;
  final boolean updateActivity;
  final boolean metaActivity;
  final long activityWindow;

  final boolean parseAttributes;

  final Long maxpastDefault;
  final Long maxfutureDefault;
  final Long maxpastOverride;
  final Long maxfutureOverride;
  final boolean ignoreOutOfRange;

  final boolean allowDeltaAttributes;

  public Ingress(KeyStore keyStore, Properties props) {

    //
    // Enable the ThrottlingManager
    //

    ThrottlingManager.enable();

    this.properties = props;

    //
    // Make sure all required configuration is present
    //

    for (String required: REQUIRED_PROPERTIES) {
      Preconditions.checkNotNull(props.getProperty(required), "Missing configuration parameter '%s'.", required);
    }

    //
    // Extract parameters from 'props'
    //

    this.allowDeltaAttributes = "true".equals(props.getProperty(Configuration.INGRESS_ATTRIBUTES_ALLOWDELTA));
    this.ttl = Long.parseLong(props.getProperty(Configuration.INGRESS_HBASE_CELLTTL, "-1"));
    this.useDatapointTs = "true".equals(props.getProperty(Configuration.INGRESS_HBASE_DPTS));

    this.doShuffle = "true".equals(props.getProperty(Configuration.INGRESS_DELETE_SHUFFLE));

    this.rejectDelete = "true".equals(props.getProperty(Configuration.INGRESS_DELETE_REJECT));

    this.activityWindow = Long.parseLong(properties.getProperty(Configuration.INGRESS_ACTIVITY_WINDOW, "0"));
    this.activityTracking = this.activityWindow > 0;
    this.updateActivity = "true".equals(props.getProperty(Configuration.INGRESS_ACTIVITY_UPDATE));
    this.metaActivity = "true".equals(props.getProperty(Configuration.INGRESS_ACTIVITY_META));

    if (props.containsKey(Configuration.INGRESS_CACHE_DUMP_PATH)) {
      this.cacheDumpPath = props.getProperty(Configuration.INGRESS_CACHE_DUMP_PATH);
    } else {
      this.cacheDumpPath = null;
    }

    if (null != props.getProperty(Configuration.INGRESS_METADATA_CACHE_SIZE)) {
      this.METADATA_CACHE_SIZE = Integer.valueOf(props.getProperty(Configuration.INGRESS_METADATA_CACHE_SIZE));
    }

    this.metaTopic = props.getProperty(Configuration.INGRESS_KAFKA_META_TOPIC);

    this.dataTopic = props.getProperty(Configuration.INGRESS_KAFKA_DATA_TOPIC);

    this.DATA_MESSAGES_THRESHOLD = Long.parseLong(props.getProperty(Configuration.INGRESS_KAFKA_DATA_MAXSIZE));
    this.METADATA_MESSAGES_THRESHOLD = Long.parseLong(props.getProperty(Configuration.INGRESS_KAFKA_METADATA_MAXSIZE));
    this.maxValueSize = Long.parseLong(props.getProperty(Configuration.INGRESS_VALUE_MAXSIZE));

    if (this.maxValueSize > (this.DATA_MESSAGES_THRESHOLD / 2) - 64) {
      throw new RuntimeException("Value of '" + Configuration.INGRESS_VALUE_MAXSIZE + "' cannot exceed that half of '" + Configuration.INGRESS_KAFKA_DATA_MAXSIZE + "' minus 64.");
    }

    extractKeys(keyStore, props);

    this.classKey = SipHashInline.getKey(keyStore.getKey(KeyStore.SIPHASH_CLASS));
    this.labelsKey = SipHashInline.getKey(keyStore.getKey(KeyStore.SIPHASH_LABELS));

    this.AES_KAFKA_META = keyStore.getKey(KeyStore.AES_KAFKA_METADATA);
    this.SIPHASH_KAFKA_META = SipHashInline.getKey(keyStore.getKey(KeyStore.SIPHASH_KAFKA_METADATA));

    this.aesDataKey = keyStore.getKey(KeyStore.AES_KAFKA_DATA);
    this.siphashDataKey = SipHashInline.getKey(keyStore.getKey(KeyStore.SIPHASH_KAFKA_DATA));

    this.sendMetadataOnDelete = Boolean.parseBoolean(props.getProperty(Configuration.INGRESS_DELETE_METADATA_INCLUDE, "false"));
    this.sendMetadataOnStore = Boolean.parseBoolean(props.getProperty(Configuration.INGRESS_STORE_METADATA_INCLUDE, "false"));

    this.parseAttributes = "true".equals(props.getProperty(Configuration.INGRESS_PARSE_ATTRIBUTES));

    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_DEFAULT)) {
      maxpastDefault = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_DEFAULT));
      if (maxpastDefault < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXPAST_DEFAULT + "' MUST be positive.");
      }
    } else {
      maxpastDefault = null;
    }

    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_DEFAULT)) {
      maxfutureDefault = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_DEFAULT));
      if (maxfutureDefault < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXFUTURE_DEFAULT + "' MUST be positive.");
      }
    } else {
      maxfutureDefault = null;
    }

    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_OVERRIDE)) {
      maxpastOverride = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXPAST_OVERRIDE));
      if (maxpastOverride < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXPAST_OVERRIDE + "' MUST be positive.");
      }
    } else {
      maxpastOverride = null;
    }

    if (null != WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_OVERRIDE)) {
      maxfutureOverride = Long.parseLong(WarpConfig.getProperty(Configuration.INGRESS_MAXFUTURE_OVERRIDE));
      if (maxfutureOverride < 0) {
        throw new RuntimeException("Value of '" + Configuration.INGRESS_MAXFUTURE_OVERRIDE + "' MUST be positive.");
      }
    } else {
      maxfutureOverride = null;
    }

    this.ignoreOutOfRange = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_OUTOFRANGE_IGNORE));

    //
    // Prepare meta, data and delete producers
    //

    Properties metaProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    metaProps.setProperty("metadata.broker.list", props.getProperty(Configuration.INGRESS_KAFKA_META_BROKERLIST));
    if (null != props.getProperty(Configuration.INGRESS_KAFKA_META_PRODUCER_CLIENTID)) {
      metaProps.setProperty("client.id", props.getProperty(Configuration.INGRESS_KAFKA_META_PRODUCER_CLIENTID));
    }
    metaProps.setProperty("request.required.acks", "-1");
    // TODO(hbs): when we move to the new KafkaProducer API
    //metaProps.setProperty(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
    metaProps.setProperty("producer.type", "sync");
    metaProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    metaProps.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());
    //??? metaProps.setProperty("block.on.buffer.full", "true");

    // FIXME(hbs): compression does not work
    //metaProps.setProperty("compression.codec", "snappy");
    //metaProps.setProperty("client.id","");

    ProducerConfig metaConfig = new ProducerConfig(metaProps);

    this.metaProducerPool = new KafkaProducerPool(metaConfig,
        Integer.parseInt(props.getProperty(Configuration.INGRESS_KAFKA_METADATA_POOLSIZE)),
        SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_POOL_GET,
        SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_WAIT_NANO);

    Properties dataProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    dataProps.setProperty("metadata.broker.list", props.getProperty(Configuration.INGRESS_KAFKA_DATA_BROKERLIST));
    if (null != props.getProperty(Configuration.INGRESS_KAFKA_DATA_PRODUCER_CLIENTID)) {
      dataProps.setProperty("client.id", props.getProperty(Configuration.INGRESS_KAFKA_DATA_PRODUCER_CLIENTID));
    }
    dataProps.setProperty("request.required.acks", "-1");
    dataProps.setProperty("producer.type", "sync");
    dataProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    dataProps.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());
    // TODO(hbs): when we move to the new KafkaProducer API
    //dataProps.setProperty(org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");

    if (null != props.getProperty(Configuration.INGRESS_KAFKA_DATA_REQUEST_TIMEOUT_MS)) {
      dataProps.setProperty("request.timeout.ms", props.getProperty(Configuration.INGRESS_KAFKA_DATA_REQUEST_TIMEOUT_MS));
    }

    ///???? dataProps.setProperty("block.on.buffer.full", "true");

    // FIXME(hbs): compression does not work
    //dataProps.setProperty("compression.codec", "snappy");
    //dataProps.setProperty("client.id","");

    ProducerConfig dataConfig = new ProducerConfig(dataProps);

    //this.dataProducer = new Producer<byte[], byte[]>(dataConfig);

    //
    // Allocate producer pool
    //

    this.dataProducers = new Producer[Integer.parseInt(props.getProperty(Configuration.INGRESS_KAFKA_DATA_POOLSIZE))];

    for (int i = 0; i < dataProducers.length; i++) {
      this.dataProducers[i] = new Producer<byte[], byte[]>(dataConfig);
    }

    this.dataProducersCurrentPoolSize = this.dataProducers.length;

    //
    // Producer for the Delete topic
    //

    /*
    Properties deleteProps = new Properties();
    // @see http://kafka.apache.org/documentation.html#producerconfigs
    deleteProps.setProperty("metadata.broker.list", props.getProperty(INGRESS_KAFKA_DELETE_BROKERLIST));
    deleteProps.setProperty("request.required.acks", "-1");
    deleteProps.setProperty("producer.type","sync");
    deleteProps.setProperty("serializer.class", "kafka.serializer.DefaultEncoder");
    deleteProps.setProperty("partitioner.class", io.warp10.continuum.KafkaPartitioner.class.getName());
    
    ProducerConfig deleteConfig = new ProducerConfig(deleteProps);
    this.deleteProducer = new Producer<byte[], byte[]>(deleteConfig);
*/

    //
    // Attempt to load the cache file (we do that prior to starting the Kafka consumer)
    //

    loadCache();

    //
    // Create Kafka consumer to handle Metadata deletions
    //

    ConsumerFactory metadataConsumerFactory = new IngressMetadataConsumerFactory(this);

    if (props.containsKey(Configuration.INGRESS_KAFKA_META_GROUPID)) {
      pool = new KafkaSynchronizedConsumerPool(props.getProperty(Configuration.INGRESS_KAFKA_META_ZKCONNECT),
          props.getProperty(Configuration.INGRESS_KAFKA_META_TOPIC),
          props.getProperty(Configuration.INGRESS_KAFKA_META_CONSUMER_CLIENTID),
          props.getProperty(Configuration.INGRESS_KAFKA_META_GROUPID),
          props.getProperty(Configuration.INGRESS_KAFKA_META_CONSUMER_PARTITION_ASSIGNMENT_STRATEGY),
          props.getProperty(Configuration.INGRESS_KAFKA_META_CONSUMER_AUTO_OFFSET_RESET),
          Integer.parseInt(props.getProperty(Configuration.INGRESS_KAFKA_META_NTHREADS)),
          Long.parseLong(props.getProperty(Configuration.INGRESS_KAFKA_META_COMMITPERIOD)),
          metadataConsumerFactory);
    } else {
      pool = null;
    }

    //
    // Initialize ThriftDirectoryService
    //

    try {
      this.directoryClient = new ThriftDirectoryClient(keyStore, props);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    //
    // Register shutdown hook
    //

    final Ingress self = this;

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        //
        // Make sure the Kakfa consumers are stopped so we don't miss deletions
        // when restarting and using the cache we are about to store
        //

        if (null != self.pool) {
          self.pool.shutdown();

          LOG.info("Waiting for Ingress Kafka consumers to stop.");

          while (!self.pool.isStopped()) {
            LockSupport.parkNanos(250000000L);
          }

          LOG.info("Kafka consumers stopped, dumping GTS cache");
        }


        self.dumpCache();
      }
    });

    //
    // Make sure ShutdownHookManager is initialized, otherwise it will try to
    // register a shutdown hook during the shutdown hook we just registered...
    //

    ShutdownHookManager.get();

    //
    // Start Jetty server
    //

    int maxThreads = Integer.parseInt(props.getProperty(Configuration.INGRESS_JETTY_THREADPOOL));

    boolean enableStreamUpdate = !("true".equals(props.getProperty(Configuration.WARP_STREAMUPDATE_DISABLE)));

    BlockingArrayQueue<Runnable> queue = null;

    if (props.containsKey(Configuration.INGRESS_JETTY_MAXQUEUESIZE)) {
      int queuesize = Integer.parseInt(props.getProperty(Configuration.INGRESS_JETTY_MAXQUEUESIZE));
      queue = new BlockingArrayQueue<Runnable>(queuesize);
    }

    Server server = new Server(new QueuedThreadPool(maxThreads, 8, 60000, queue));

    List<Connector> connectors = new ArrayList<Connector>();

    boolean useHttp = null != props.getProperty(Configuration.INGRESS_PORT);
    boolean useHttps = null != props.getProperty(Configuration.INGRESS_PREFIX + Configuration._SSL_PORT);

    if (useHttp) {
      int port = Integer.valueOf(props.getProperty(Configuration.INGRESS_PORT));
      String host = props.getProperty(Configuration.INGRESS_HOST);
      int tcpBacklog = Integer.valueOf(props.getProperty(Configuration.INGRESS_TCP_BACKLOG, "0"));
      int acceptors = Integer.valueOf(props.getProperty(Configuration.INGRESS_ACCEPTORS));
      int selectors = Integer.valueOf(props.getProperty(Configuration.INGRESS_SELECTORS));
      long idleTimeout = Long.parseLong(props.getProperty(Configuration.INGRESS_IDLE_TIMEOUT));

      ServerConnector connector = new ServerConnector(server, acceptors, selectors);
      connector.setIdleTimeout(idleTimeout);
      connector.setPort(port);
      connector.setHost(host);
      connector.setAcceptQueueSize(tcpBacklog);
      connector.setName("Continuum Ingress HTTP");

      connectors.add(connector);
    }

    if (useHttps) {
      ServerConnector connector = SSLUtils.getConnector(server, Configuration.INGRESS_PREFIX);
      connector.setName("Continuum Ingress HTTPS");
      connectors.add(connector);
    }

    server.setConnectors(connectors.toArray(new Connector[connectors.size()]));

    HandlerList handlers = new HandlerList();

    Handler cors = new CORSHandler();
    handlers.addHandler(cors);

    handlers.addHandler(new IngressUpdateHandler(this));
    handlers.addHandler(new IngressMetaHandler(this));
    handlers.addHandler(new IngressDeleteHandler(this));

    if (enableStreamUpdate) {
      IngressStreamUpdateHandler suHandler = new IngressStreamUpdateHandler(this);
      handlers.addHandler(suHandler);
    }

    server.setHandler(handlers);

    JettyUtil.setSendServerVersion(server, false);

    try {
      server.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Thread t = new Thread(this);
    t.setDaemon(true);
    t.setName("Continuum Ingress");
    t.start();
  }

  @Override
  public void run() {
    //
    // Register in ZK and watch parent znode.
    // If the Ingress count exceeds the licensed number,
    // exit if we are the first of the list.
    //

    while (true) {
      try {
        Thread.sleep(Long.MAX_VALUE);
      } catch (InterruptedException ie) {
      }
    }
  }

  /**
   * Extract Ingress related keys and populate the KeyStore with them.
   *
   * @param props Properties from which to extract the key specs
   */
  private static void extractKeys(KeyStore keyStore, Properties props) {
    String keyspec = props.getProperty(Configuration.INGRESS_KAFKA_META_MAC);

    if (null != keyspec) {
      byte[] key = keyStore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.INGRESS_KAFKA_META_MAC + " MUST be 128 bits long.");
      keyStore.setKey(KeyStore.SIPHASH_KAFKA_METADATA, key);
    }

    keyspec = props.getProperty(Configuration.INGRESS_KAFKA_DATA_MAC);

    if (null != keyspec) {
      byte[] key = keyStore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.INGRESS_KAFKA_DATA_MAC + " MUST be 128 bits long.");
      keyStore.setKey(KeyStore.SIPHASH_KAFKA_DATA, key);
    }

    keyspec = props.getProperty(Configuration.INGRESS_KAFKA_META_AES);

    if (null != keyspec) {
      byte[] key = keyStore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.INGRESS_KAFKA_META_AES + " MUST be 128, 192 or 256 bits long.");
      keyStore.setKey(KeyStore.AES_KAFKA_METADATA, key);
    }

    keyspec = props.getProperty(Configuration.INGRESS_KAFKA_DATA_AES);

    if (null != keyspec) {
      byte[] key = keyStore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length || 24 == key.length || 32 == key.length, "Key " + Configuration.INGRESS_KAFKA_DATA_AES + " MUST be 128, 192 or 256 bits long.");
      keyStore.setKey(KeyStore.AES_KAFKA_DATA, key);
    }

    keyspec = props.getProperty(Configuration.DIRECTORY_PSK);

    if (null != keyspec) {
      byte[] key = keyStore.decodeKey(keyspec);
      Preconditions.checkArgument(16 == key.length, "Key " + Configuration.DIRECTORY_PSK + " MUST be 128 bits long.");
      keyStore.setKey(KeyStore.SIPHASH_DIRECTORY_PSK, key);
    }

    keyStore.forget();
  }

  void pushMetadataMessage(Metadata metadata) throws IOException {

    if (null == metadata) {
      pushMetadataMessage(null, null);
      return;
    }

    //
    // Compute class/labels Id
    //
    // 128bits
    metadata.setClassId(GTSHelper.classId(this.classKey, metadata.getName()));
    metadata.setLabelsId(GTSHelper.labelsId(this.labelsKey, metadata.getLabels()));

    TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
    try {
      byte[] bytes = new byte[16];
      GTSHelper.fillGTSIds(bytes, 0, metadata.getClassId(), metadata.getLabelsId());
      pushMetadataMessage(bytes, serializer.serialize(metadata));
    } catch (TException te) {
      throw new IOException("Unable to push metadata.");
    }
  }

  /**
   * Push a metadata message onto the buffered list of Kafka messages
   * and flush the list to Kafka if it has reached a threshold.
   *
   * @param key   Key of the message to queue
   * @param value Value of the message to queue
   */
  void pushMetadataMessage(byte[] key, byte[] value) throws IOException {

    AtomicLong mms = this.metadataMessagesSize.get();
    List<KeyedMessage<byte[], byte[]>> msglist = this.metadataMessages.get();

    if (null != key && null != value) {

      //
      // Add key as a prefix of value
      //

      byte[] kv = Arrays.copyOf(key, key.length + value.length);
      System.arraycopy(value, 0, kv, key.length, value.length);
      value = kv;

      //
      // Encrypt value if the AES key is defined
      //

      if (null != this.AES_KAFKA_META) {
        value = CryptoUtils.wrap(this.AES_KAFKA_META, value);
      }

      //
      // Compute MAC if the SipHash key is defined
      //

      if (null != this.SIPHASH_KAFKA_META) {
        value = CryptoUtils.addMAC(this.SIPHASH_KAFKA_META, value);
      }

      KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(this.metaTopic, Arrays.copyOf(key, key.length), value);
      msglist.add(message);
      mms.addAndGet(key.length + value.length);

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_META_MESSAGES, Sensision.EMPTY_LABELS, 1);
    }

    if (msglist.size() > 0 && (null == key || null == value || mms.get() > METADATA_MESSAGES_THRESHOLD)) {
      Producer<byte[], byte[]> producer = this.metaProducerPool.getProducer();
      try {

        //
        // How long it takes to send messages to Kafka
        //

        long nano = System.nanoTime();

        producer.send(msglist);

        nano = System.nanoTime() - nano;
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_METADATA_PRODUCER_SEND, Sensision.EMPTY_LABELS, nano);

      } catch (Throwable t) {
        //
        // We need to remove the IDs of Metadata in 'msglist' from the cache so they get a chance to be
        // pushed later
        //

        for (KeyedMessage<byte[], byte[]> msg: msglist) {
          synchronized (this.metadataCache) {
            this.metadataCache.remove(new BigInteger(msg.key()));
          }
        }

        throw t;
      } finally {
        this.metaProducerPool.recycleProducer(producer);
      }

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_META_SEND, Sensision.EMPTY_LABELS, 1);
      msglist.clear();
      mms.set(0L);
      // Update sensision metric with size of metadata cache
      Sensision.set(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_METADATA_CACHED, Sensision.EMPTY_LABELS, this.metadataCache.size());
    }
  }

  /**
   * Push a metadata message onto the buffered list of Kafka messages
   * and flush the list to Kafka if it has reached a threshold.
   *
   * @param encoder GTSEncoder to push to Kafka. It MUST have classId/labelsId set.
   */
  void pushDataMessage(GTSEncoder encoder, Map<String, String> attributes) throws IOException {
    if (null != encoder) {
      KafkaDataMessage msg = new KafkaDataMessage();
      msg.setType(KafkaDataMessageType.STORE);
      msg.setData(encoder.getBytes());
      msg.setClassId(encoder.getClassId());
      msg.setLabelsId(encoder.getLabelsId());

      if (this.sendMetadataOnStore) {
        msg.setMetadata(encoder.getMetadata());
      }

      if (null != attributes && !attributes.isEmpty()) {
        msg.setAttributes(new HashMap<String, String>(attributes));
      }
      sendDataMessage(msg);
    } else {
      sendDataMessage(null);
    }
  }

  private void sendDataMessage(KafkaDataMessage msg) throws IOException {
    AtomicLong dms = this.dataMessagesSize.get();
    List<KeyedMessage<byte[], byte[]>> msglist = this.dataMessages.get();

    if (null != msg) {
      //
      // Build key
      //

      byte[] bytes = new byte[16];

      GTSHelper.fillGTSIds(bytes, 0, msg.getClassId(), msg.getLabelsId());

      //ByteBuffer bb = ByteBuffer.wrap(new byte[16]).order(ByteOrder.BIG_ENDIAN);
      //bb.putLong(encoder.getClassId());
      //bb.putLong(encoder.getLabelsId());

      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

      byte[] msgbytes = null;

      try {
        msgbytes = serializer.serialize(msg);
      } catch (TException te) {
        throw new IOException(te);
      }

      //
      // Encrypt value if the AES key is defined
      //

      if (null != this.aesDataKey) {
        msgbytes = CryptoUtils.wrap(this.aesDataKey, msgbytes);
      }

      //
      // Compute MAC if the SipHash key is defined
      //

      if (null != this.siphashDataKey) {
        msgbytes = CryptoUtils.addMAC(this.siphashDataKey, msgbytes);
      }

      //KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(this.dataTopic, bb.array(), msgbytes);
      KeyedMessage<byte[], byte[]> message = new KeyedMessage<byte[], byte[]>(this.dataTopic, bytes, msgbytes);
      msglist.add(message);
      //this.dataMessagesSize.get().addAndGet(bb.array().length + msgbytes.length);      
      dms.addAndGet(bytes.length + msgbytes.length);

      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_MESSAGES, Sensision.EMPTY_LABELS, 1);
    }

    if (msglist.size() > 0 && (null == msg || dms.get() > DATA_MESSAGES_THRESHOLD)) {
      Producer<byte[], byte[]> producer = getDataProducer();
      //this.dataProducer.send(msglist);
      try {

        //
        // How long it takes to send messages to Kafka
        //

        long nano = System.nanoTime();

        producer.send(msglist);

        nano = System.nanoTime() - nano;
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_SEND, Sensision.EMPTY_LABELS, nano);

      } catch (Throwable t) {
        throw t;
      } finally {
        recycleDataProducer(producer);
      }
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_SEND, Sensision.EMPTY_LABELS, 1);
      msglist.clear();
      dms.set(0L);
    }
  }

  private Producer<byte[], byte[]> getDataProducer() {

    //
    // We will count how long we wait for a producer
    //

    long nano = System.nanoTime();

    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_POOL_GET, Sensision.EMPTY_LABELS, 1);

    while (true) {
      synchronized (this.dataProducers) {
        if (this.dataProducersCurrentPoolSize > 0) {
          //
          // hand out the producer at index 0
          //

          Producer<byte[], byte[]> producer = this.dataProducers[0];

          //
          // Decrement current pool size
          //

          this.dataProducersCurrentPoolSize--;

          //
          // Move the last element of the array at index 0
          //

          this.dataProducers[0] = this.dataProducers[this.dataProducersCurrentPoolSize];
          this.dataProducers[this.dataProducersCurrentPoolSize] = null;

          //
          // Log waiting time
          //

          nano = System.nanoTime() - nano;
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_KAFKA_DATA_PRODUCER_WAIT_NANO, Sensision.EMPTY_LABELS, nano);

          return producer;
        }
      }

      LockSupport.parkNanos(500000L);
    }
  }

  private void recycleDataProducer(Producer<byte[], byte[]> producer) {

    if (this.dataProducersCurrentPoolSize == this.dataProducers.length) {
      throw new RuntimeException("Invalid call to recycleProducer, pool already full!");
    }

    synchronized (this.dataProducers) {
      //
      // Add the recycled producer at the end of the pool
      //

      this.dataProducers[this.dataProducersCurrentPoolSize++] = producer;
    }
  }


  /**
   * Push a deletion message onto the buffered list of Kafka messages
   * and flush the list to Kafka if it has reached a threshold.
   * <p>
   * Deletion messages MUST be pushed onto the data topic, otherwise the
   * ordering won't be respected and you risk deleting a GTS which has been
   * since repopulated with data.
   *
   * @param start    Start timestamp for deletion
   * @param end      End timestamp for deletion
   * @param metadata Metadata of the GTS to delete
   */
  void pushDeleteMessage(long start, long end, long minage, Metadata metadata) throws IOException {
    if (null != metadata) {
      KafkaDataMessage msg = new KafkaDataMessage();
      msg.setType(KafkaDataMessageType.DELETE);
      msg.setDeletionStartTimestamp(start);
      msg.setDeletionEndTimestamp(end);
      msg.setDeletionMinAge(minage);
      msg.setClassId(metadata.getClassId());
      msg.setLabelsId(metadata.getLabelsId());

      if (this.sendMetadataOnDelete) {
        msg.setMetadata(metadata);
      }

      sendDataMessage(msg);
    } else {
      sendDataMessage(null);
    }
  }

  private void dumpCache() {
    if (null == this.cacheDumpPath) {
      return;
    }

    OutputStream out = null;

    long nano = System.nanoTime();
    long count = 0;

    try {
      out = new GZIPOutputStream(new FileOutputStream(this.cacheDumpPath));

      Set<BigInteger> bis = new HashSet<BigInteger>();

      synchronized (this.metadataCache) {
        boolean error = false;
        do {
          try {
            error = false;
            bis.addAll(this.metadataCache.keySet());
          } catch (ConcurrentModificationException cme) {
            error = true;
          }
        } while (error);
      }

      Iterator<BigInteger> iter = bis.iterator();

      //
      // 128bits
      //

      byte[] allzeroes = new byte[16];
      Arrays.fill(allzeroes, (byte) 0);
      byte[] allones = new byte[16];
      Arrays.fill(allones, (byte) 0xff);

      while (true) {
        try {
          if (!iter.hasNext()) {
            break;
          }
          BigInteger bi = iter.next();

          byte[] raw = bi.toByteArray();

          //
          // 128bits
          //

          if (raw.length != 16) {
            if (bi.signum() < 0) {
              out.write(allones, 0, 16 - raw.length);
            } else {
              out.write(allzeroes, 0, 16 - raw.length);
            }
          }

          out.write(raw);

          if (this.activityTracking) {
            Long lastActivity = this.metadataCache.get(bi);
            byte[] bytes;
            if (null != lastActivity) {
              bytes = Longs.toByteArray(lastActivity);
            } else {
              bytes = new byte[8];
            }
            out.write(bytes);
          }
          count++;
        } catch (ConcurrentModificationException cme) {
        }
      }
    } catch (IOException ioe) {
    } finally {
      if (null != out) {
        try {
          out.close();
        } catch (Exception e) {
        }
      }
    }

    nano = System.nanoTime() - nano;

    LOG.info("Dumped " + count + " cache entries in " + (nano / 1000000.0D) + " ms.");
  }

  private void loadCache() {
    if (null == this.cacheDumpPath) {
      return;
    }

    InputStream in = null;

    byte[] buf = new byte[8192];

    long nano = System.nanoTime();
    long count = 0;

    try {
      in = new GZIPInputStream(new FileInputStream(this.cacheDumpPath));

      int offset = 0;

      // 128 bits
      int reclen = this.activityTracking ? 24 : 16;
      byte[] raw = new byte[16];

      while (true) {
        int len = in.read(buf, offset, buf.length - offset);

        offset += len;

        int idx = 0;

        while (idx < offset && offset - idx >= reclen) {
          System.arraycopy(buf, idx, raw, 0, 16);
          BigInteger id = new BigInteger(raw);
          if (this.activityTracking) {
            long lastActivity = 0L;

            for (int i = 0; i < 8; i++) {
              lastActivity <<= 8;
              lastActivity |= ((long) buf[idx + 16 + i]) & 0xFFL;
            }
            synchronized (this.metadataCache) {
              this.metadataCache.put(id, lastActivity);
            }
          } else {
            synchronized (this.metadataCache) {
              this.metadataCache.put(id, null);
            }
          }
          count++;
          idx += reclen;
        }

        if (idx < offset) {
          for (int i = idx; i < offset; i++) {
            buf[i - idx] = buf[i];
          }
          offset = offset - idx;
        } else {
          offset = 0;
        }

        if (len < 0) {
          break;
        }
      }
    } catch (IOException ioe) {
    } finally {
      if (null != in) {
        try {
          in.close();
        } catch (Exception e) {
        }
      }
    }

    nano = System.nanoTime() - nano;

    LOG.info("Loaded " + count + " cache entries in " + (nano / 1000000.0D) + " ms.");
  }
}
