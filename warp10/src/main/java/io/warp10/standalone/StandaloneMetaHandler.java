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

package io.warp10.standalone;

import io.warp10.ThrowableUtils;
import io.warp10.WarpConfig;
import io.warp10.WarpManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.ingress.DatalogForwarder;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.StoreClient;
import io.warp10.continuum.store.thrift.data.DatalogRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.CryptoUtils;
import io.warp10.crypto.KeyStore;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;
import org.apache.commons.io.output.FileWriterWithEncoding;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class StandaloneMetaHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneMetaHandler.class);

  /**
   * Default max size of value
   */
  public static final String DEFAULT_VALUE_MAXSIZE = "65536";

  private final StandaloneDirectoryClient directoryClient;

  /**
   * Key to wrap the token in the file names
   */
  private final byte[] datalogPSK;

  private final File loggingDir;

  private final String datalogId;

  private final boolean logforwarded;

  private final DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyyMMdd'T'HHmmss.SSS").withZoneUTC();

  private final boolean metaActivity;

  private final Long maxpastDefault;
  private final Long maxfutureDefault;
  private final Long maxpastOverride;
  private final Long maxfutureOverride;

  private final boolean allowDeltaAttributes;

  public StandaloneMetaHandler(KeyStore keyStore, StandaloneDirectoryClient directoryClient, StoreClient storeClient) {
    this.directoryClient = directoryClient;

    this.allowDeltaAttributes = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_ATTRIBUTES_ALLOWDELTA));

    metaActivity = "true".equals(WarpConfig.getProperty(Configuration.INGRESS_ACTIVITY_META));

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

    String dirProp = WarpConfig.getProperty(Configuration.DATALOG_DIR);
    if (null != dirProp) {
      File dir = new File(dirProp);

      if (!dir.exists()) {
        throw new RuntimeException("Data logging target '" + dir + "' does not exist.");
      } else if (!dir.isDirectory()) {
        throw new RuntimeException("Data logging target '" + dir + "' is not a directory.");
      } else {
        loggingDir = dir;
        LOG.info("Data logging enabled in directory '" + dir + "'.");
      }

      String id = WarpConfig.getProperty(Configuration.DATALOG_ID);

      if (null == id) {
        throw new RuntimeException("Property '" + Configuration.DATALOG_ID + "' MUST be set to a unique value for this instance.");
      } else {
        datalogId = new String(OrderPreservingBase64.encode(id.getBytes(StandardCharsets.UTF_8)), StandardCharsets.US_ASCII);
      }

    } else {
      loggingDir = null;
      datalogId = null;
    }

    String pskDir = WarpConfig.getProperty(Configuration.DATALOG_PSK);
    if (null != pskDir) {
      this.datalogPSK = keyStore.decodeKey(pskDir);
    } else {
      this.datalogPSK = null;
    }

    this.logforwarded = "true".equals(WarpConfig.getProperty(Configuration.DATALOG_LOGFORWARDED));
  }

  /**
   * Handle Metadata updating
   */
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    if (target.equals(Constants.API_ENDPOINT_META)) {
      baseRequest.setHandled(true);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_META_REQUESTS, Sensision.EMPTY_LABELS, 1);
    } else {
      return;
    }

    if (null != WarpManager.getAttribute(WarpManager.META_DISABLED)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, String.valueOf(WarpManager.getAttribute(WarpManager.META_DISABLED)));
      return;
    }

    long lastActivity = System.currentTimeMillis();

    //
    // CORS header
    //

    response.setHeader("Access-Control-Allow-Origin", "*");

    boolean deltaAttributes = "delta".equals(request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_ATTRIBUTES)));

    if (deltaAttributes && !this.allowDeltaAttributes) {
      throw new IOException("Delta update of attributes is disabled.");
    }

    try {
      //
      // Extract DatalogRequest if specified
      //

      String datalogHeader = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_DATALOG));

      DatalogRequest dr = null;

      boolean forwarded = false;

      if (null != datalogHeader) {
        byte[] bytes = OrderPreservingBase64.decode(datalogHeader.getBytes(StandardCharsets.US_ASCII));

        if (null != datalogPSK) {
          bytes = CryptoUtils.unwrap(datalogPSK, bytes);
        }

        if (null == bytes) {
          throw new IOException("Invalid Datalog header.");
        }

        TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());

        try {
          dr = new DatalogRequest();
          deser.deserialize(dr, bytes);
        } catch (TException te) {
          throw new IOException();
        }

        // Set lastActivity to the timestamp of the DatalogRequest
        lastActivity = dr.getTimestamp() / 1000000L;

        Map<String, String> labels = new HashMap<String, String>();
        labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(dr.getId().getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8));
        labels.put(SensisionConstants.SENSISION_LABEL_TYPE, dr.getType());
        Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_REQUESTS_RECEIVED, labels, 1);

        forwarded = true;

        deltaAttributes = dr.isDeltaAttributes();
      }

      //
      // TODO(hbs): Extract producer/owner from token
      //

      String token = null != dr ? dr.getToken() : request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));

      WriteToken writeToken;

      try {
        writeToken = Tokens.extractWriteToken(token);
        if (writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_NOMETA)) {
          throw new WarpScriptException("Token cannot be used for updating metadata.");
        }
      } catch (WarpScriptException ee) {
        throw new IOException(ee);
      }

      String application = writeToken.getAppName();
      String producer = Tokens.getUUID(writeToken.getProducerId());
      String owner = Tokens.getUUID(writeToken.getOwnerId());

      if (null == producer || null == owner) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_META_INVALIDTOKEN, Sensision.EMPTY_LABELS, 1);
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }

      Map<String, String> sensisionLabels = new HashMap<String, String>();
      sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);

      if (null != application) {
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }

      //
      // Determine if content if gzipped
      //

      boolean gzipped = false;

      if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_META_REQUESTS, Sensision.EMPTY_LABELS, 1);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_STANDALONE_META_GZIPPED, sensisionLabels, 1);
        gzipped = true;
      }

      BufferedReader br = null;

      if (gzipped) {
        GZIPInputStream is = new GZIPInputStream(request.getInputStream());
        br = new BufferedReader(new InputStreamReader(is));
      } else {
        br = request.getReader();
      }

      File loggingFile = null;
      PrintWriter loggingWriter = null;

      //
      // Open the logging file if logging is enabled
      //

      if (null != loggingDir) {
        long nanos = null != dr ? dr.getTimestamp() : TimeSource.getNanoTime();
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toHexString(nanos));
        sb.insert(0, "0000000000000000", 0, 16 - sb.length());
        sb.append("-");
        if (null != dr) {
          sb.append(dr.getId());
        } else {
          sb.append(datalogId);
        }

        sb.append("-");
        sb.append(dtf.print(nanos / 1000000L));
        sb.append(Long.toString(1000000L + (nanos % 1000000L)).substring(1));
        sb.append("Z");

        if (null == dr) {
          dr = new DatalogRequest();
          dr.setTimestamp(nanos);
          dr.setType(Constants.DATALOG_META);
          dr.setId(datalogId);
          dr.setToken(token);
          dr.setDeltaAttributes(deltaAttributes);
        }

        if (null != dr && (!forwarded || (forwarded && this.logforwarded))) {
          //
          // Serialize the request
          //

          TSerializer ser = new TSerializer(new TCompactProtocol.Factory());

          byte[] encoded;

          try {
            encoded = ser.serialize(dr);
          } catch (TException te) {
            throw new IOException(te);
          }

          if (null != this.datalogPSK) {
            encoded = CryptoUtils.wrap(this.datalogPSK, encoded);
          }

          encoded = OrderPreservingBase64.encode(encoded);

          loggingFile = new File(loggingDir, sb.toString());
          loggingWriter = new PrintWriter(new FileWriterWithEncoding(loggingFile, StandardCharsets.UTF_8));

          //
          // Write request
          //

          loggingWriter.println(new String(encoded, StandardCharsets.US_ASCII));
        }
      }

      try {
        //
        // Loop over the input lines.
        // Each has the following format:
        //
        // class{labels}{attributes}
        //

        while (true) {
          String line = br.readLine();

          if (null == line) {
            break;
          }

          line = line.trim();

          if (0 == line.length()) {
            continue;
          }

          //
          // Ignore comments
          //

          if ('#' == line.charAt(0)) {
            continue;
          }

          Metadata metadata = MetadataUtils.parseMetadata(line);

          // Add labels from the WriteToken if they exist
          if (writeToken.getLabelsSize() > 0) {
            metadata.getLabels().putAll(writeToken.getLabels());
          }
          //
          // Force owner/producer
          //

          metadata.getLabels().put(Constants.PRODUCER_LABEL, producer);
          metadata.getLabels().put(Constants.OWNER_LABEL, owner);

          if (null != application) {
            metadata.getLabels().put(Constants.APPLICATION_LABEL, application);
          } else {
            // remove application label
            metadata.getLabels().remove(Constants.APPLICATION_LABEL);
          }

          if (!MetadataUtils.validateMetadata(metadata)) {
            response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid metadata " + metadata);
            return;
          }

          if (deltaAttributes) {
            metadata.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);
          } else {
            metadata.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
          }

          if (metaActivity) {
            metadata.setLastActivity(lastActivity);
          }

          this.directoryClient.register(metadata);

          //
          // Write the line last, so we do not write lines which triggered exceptions
          //

          if (null != loggingWriter) {
            loggingWriter.println(line);
          }
        }
      } finally {
        if (null != loggingWriter) {
          Map<String, String> labels = new HashMap<String, String>();
          labels.put(SensisionConstants.SENSISION_LABEL_ID, new String(OrderPreservingBase64.decode(dr.getId().getBytes(StandardCharsets.US_ASCII)), StandardCharsets.UTF_8));
          labels.put(SensisionConstants.SENSISION_LABEL_TYPE, dr.getType());
          Sensision.update(SensisionConstants.CLASS_WARP_DATALOG_REQUESTS_LOGGED, labels, 1);

          loggingWriter.close();
          // Create hard links when multiple datalog forwarders are configured
          for (Path srcDir: Warp.getDatalogSrcDirs()) {
            try {
              Files.createLink(new File(srcDir.toFile(), loggingFile.getName() + DatalogForwarder.DATALOG_SUFFIX).toPath(), loggingFile.toPath());
            } catch (Exception e) {
              throw new RuntimeException("Encountered an error while attempting to link " + loggingFile + " to " + srcDir);
            }
          }
          //loggingFile.renameTo(new File(loggingFile.getAbsolutePath() + DatalogForwarder.DATALOG_SUFFIX));
          loggingFile.delete();
        }
        this.directoryClient.register(null);
      }

      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Throwable t) { // Catch everything else this handler could return 200 on a OOM exception
      if (!response.isCommitted()) {
        String prefix = "Error when updating meta: ";
        String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
        return;
      }
    }
  }
}
