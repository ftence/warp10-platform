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

import io.warp10.ThrowableUtils;
import io.warp10.WarpManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.ThrottlingManager;
import io.warp10.continuum.TimeSource;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.WarpException;
import io.warp10.continuum.gts.GTSEncoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;

public class IngressUpdateHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(IngressUpdateHandler.class);

  private final Ingress ingress;

  public IngressUpdateHandler(Ingress ingress) {
    this.ingress = ingress;
  }

  /**
   * Handle data updating
   */
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
    //
    // Only handle /update and /update/token
    //

    String token = null;

    if (!target.equals(Constants.API_ENDPOINT_UPDATE)) {
      if (target.startsWith(Constants.API_ENDPOINT_UPDATE + "/")) {
        token = target.substring(Constants.API_ENDPOINT_UPDATE.length() + 1);
      } else {
        return;
      }
    }

    //
    // Make the request as handled
    //

    baseRequest.setHandled(true);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_REQUESTS, Sensision.EMPTY_LABELS, 1);

    //
    // Check the endpoint is activated
    //

    if (null != WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, String.valueOf(WarpManager.getAttribute(WarpManager.UPDATE_DISABLED)));
      return;
    }

    // By default, no label, will be updated later.
    Map<String, String> sensisionLabels = Sensision.EMPTY_LABELS;

    try {
      // For last activity
      long nowms = System.currentTimeMillis();

      //
      // CORS header
      //

      response.setHeader("Access-Control-Allow-Origin", "*");

      long nano = System.nanoTime();

      //
      // Delta attributes
      //

      boolean deltaAttributes = "delta".equals(request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_ATTRIBUTES)));

      if (deltaAttributes && !ingress.allowDeltaAttributes) {
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Delta update of attributes is disabled.");
        return;
      }

      //
      // Extract token infos
      //

      if (null == token) {
        token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));
      }

      WriteToken writeToken;

      try {
        writeToken = Tokens.extractWriteToken(token);
        if (writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_NOUPDATE)) {
          response.sendError(HttpServletResponse.SC_FORBIDDEN, "Token cannot be used for updating data.");
          return;
        }
      } catch (WarpScriptException ee) {
        throw new IOException(ee);
      }

      String application = writeToken.getAppName();
      String producer = Tokens.getUUID(writeToken.getProducerId());
      String owner = Tokens.getUUID(writeToken.getOwnerId());

      //
      // For update operations, producer and owner MUST be non-null
      //

      if (null == producer || null == owner) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_INVALIDTOKEN, Sensision.EMPTY_LABELS, 1);
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }

      //
      // Max value size
      //

      long maxsize = ingress.maxValueSize;

      if (writeToken.getAttributesSize() > 0 && null != writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXSIZE)) {
        maxsize = Long.parseLong(writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXSIZE));
        if (maxsize > (ingress.DATA_MESSAGES_THRESHOLD / 2) - 64) {
          maxsize = (ingress.DATA_MESSAGES_THRESHOLD / 2) - 64;
        }
      }

      //
      // Sensision labels (producer and application).
      //

      sensisionLabels = new HashMap<String, String>();
      sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);
      if (null != application) {
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }

      //
      // Build extra labels
      //

      Map<String, String> extraLabels = new HashMap<String, String>();

      // Add labels from the WriteToken if they exist
      if (writeToken.getLabelsSize() > 0) {
        extraLabels.putAll(writeToken.getLabels());
      }

      // Force internal labels producer and owner
      extraLabels.put(Constants.PRODUCER_LABEL, producer);
      extraLabels.put(Constants.OWNER_LABEL, owner);

      // Force internal labels application
      // FIXME(hbs): remove me, apps should be set in all tokens now...
      if (null != application) {
        extraLabels.put(Constants.APPLICATION_LABEL, application);
      } else {
        // remove application label
        extraLabels.remove(Constants.APPLICATION_LABEL);
      }

      long count = 0;

      //
      // Extract KafkaDataMessage attributes
      //

      Map<String, String> kafkaDataMessageAttributes = null;

      if (-1 != ingress.ttl || ingress.useDatapointTs) {
        kafkaDataMessageAttributes = new HashMap<String, String>();
        if (-1 != ingress.ttl) {
          kafkaDataMessageAttributes.put(Constants.STORE_ATTR_TTL, Long.toString(ingress.ttl));
        }
        if (ingress.useDatapointTs) {
          kafkaDataMessageAttributes.put(Constants.STORE_ATTR_USEDATAPOINTTS, "t");
        }
      }

      if (writeToken.getAttributesSize() > 0) {
        if (writeToken.getAttributes().containsKey(Constants.STORE_ATTR_TTL)
            || writeToken.getAttributes().containsKey(Constants.STORE_ATTR_USEDATAPOINTTS)) {
          if (null == kafkaDataMessageAttributes) {
            kafkaDataMessageAttributes = new HashMap<String, String>();
          }
          if (writeToken.getAttributes().containsKey(Constants.STORE_ATTR_TTL)) {
            kafkaDataMessageAttributes.put(Constants.STORE_ATTR_TTL, writeToken.getAttributes().get(Constants.STORE_ATTR_TTL));
          }
          if (writeToken.getAttributes().containsKey(Constants.STORE_ATTR_USEDATAPOINTTS)) {
            kafkaDataMessageAttributes.put(Constants.STORE_ATTR_USEDATAPOINTTS, writeToken.getAttributes().get(Constants.STORE_ATTR_USEDATAPOINTTS));
          }
        }
      }

      try {

        //
        // Determine if content is gzipped
        //

        boolean gzipped = false;

        if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_GZIPPED, sensisionLabels, 1);
          gzipped = true;
        }

        BufferedReader br = null;

        if (gzipped) {
          GZIPInputStream is = new GZIPInputStream(request.getInputStream());
          br = new BufferedReader(new InputStreamReader(is));
        } else {
          br = request.getReader();
        }

        //
        // Get the present time
        //

        Long now = TimeSource.getTime();

        //
        // Extract time limits
        //

        Long maxpast = null;

        if (null != ingress.maxpastDefault) {
          try {
            maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, ingress.maxpastDefault));
          } catch (ArithmeticException ae) {
            maxpast = null;
          }
        }

        Long maxfuture = null;

        if (null != ingress.maxfutureDefault) {
          try {
            maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, ingress.maxfutureDefault));
          } catch (ArithmeticException ae) {
            maxfuture = null;
          }
        }

        Boolean ignoor = null;

        if (writeToken.getAttributesSize() > 0) {

          if (writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_IGNOOR)) {
            String v = writeToken.getAttributes().get(Constants.TOKEN_ATTR_IGNOOR).toLowerCase();
            if ("true".equals(v) || "t".equals(v)) {
              ignoor = Boolean.TRUE;
            } else if ("false".equals(v) || "f".equals(v)) {
              ignoor = Boolean.FALSE;
            }
          }

          String deltastr = writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXPAST);

          if (null != deltastr) {
            long delta = Long.parseLong(deltastr);
            if (delta < 0) {
              throw new WarpScriptException("Invalid '" + Constants.TOKEN_ATTR_MAXPAST + "' token attribute, MUST be positive.");
            }
            try {
              maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, delta));
            } catch (ArithmeticException ae) {
              maxpast = null;
            }
          }

          deltastr = writeToken.getAttributes().get(Constants.TOKEN_ATTR_MAXFUTURE);

          if (null != deltastr) {
            long delta = Long.parseLong(deltastr);
            if (delta < 0) {
              throw new WarpScriptException("Invalid '" + Constants.TOKEN_ATTR_MAXFUTURE + "' token attribute, MUST be positive.");
            }
            try {
              maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, delta));
            } catch (ArithmeticException ae) {
              maxfuture = null;
            }
          }
        }

        if (null != ingress.maxpastOverride) {
          try {
            maxpast = Math.subtractExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, ingress.maxpastOverride));
          } catch (ArithmeticException ae) {
            maxpast = null;
          }
        }

        if (null != ingress.maxfutureOverride) {
          try {
            maxfuture = Math.addExact(now, Math.multiplyExact(Constants.TIME_UNITS_PER_MS, ingress.maxfutureOverride));
          } catch (ArithmeticException ae) {
            maxfuture = null;
          }
        }

        //
        // Check the value of the 'now' header
        //
        // The following values are supported:
        //
        // A number, which will be interpreted as an absolute time reference,
        // i.e. a number of time units since the Epoch.
        //
        // A number prefixed by '+' or '-' which will be interpreted as a
        // delta from the present time.
        //
        // A '*' which will mean to not set 'now', and to recompute its value
        // each time it's needed.
        //

        String nowstr = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_NOW_HEADERX));

        if (null != nowstr) {
          if ("*".equals(nowstr)) {
            now = null;
          } else if (nowstr.startsWith("+")) {
            try {
              long delta = Long.parseLong(nowstr.substring(1));
              now = now + delta;
            } catch (Exception e) {
              throw new IOException("Invalid base timestamp.");
            }
          } else if (nowstr.startsWith("-")) {
            try {
              long delta = Long.parseLong(nowstr.substring(1));
              now = now - delta;
            } catch (Exception e) {
              throw new IOException("Invalid base timestamp.");
            }
          } else {
            try {
              now = Long.parseLong(nowstr);
            } catch (Exception e) {
              throw new IOException("Invalid base timestamp.");
            }
          }
        }

        //
        // Loop on all lines
        //

        GTSEncoder lastencoder = null;
        GTSEncoder encoder = null;

        byte[] bytes = new byte[16];

        AtomicLong dms = ingress.dataMessagesSize.get();

        // Atomic boolean to track if attributes were parsed
        AtomicBoolean hadAttributes = ingress.parseAttributes ? new AtomicBoolean(false) : null;

        boolean lastHadAttributes = false;

        AtomicLong ignoredCount = null;

        if ((ingress.ignoreOutOfRange && !Boolean.FALSE.equals(ignoor)) || Boolean.TRUE.equals(ignoor)) {
          ignoredCount = new AtomicLong(0L);
        }

        do {
          // We copy the current value of hadAttributes
          if (ingress.parseAttributes) {
            lastHadAttributes = lastHadAttributes || hadAttributes.get();
            hadAttributes.set(false);
          }

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

          try {
            encoder = GTSHelper.parse(lastencoder, line, extraLabels, now, maxsize, hadAttributes, maxpast, maxfuture, ignoredCount, deltaAttributes);
            count++;
          } catch (ParseException pe) {
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_PARSEERRORS, sensisionLabels, 1);
            throw new IOException("Parse error at '" + line + "'", pe);
          }

          if (encoder != lastencoder || dms.get() + 16 + lastencoder.size() > ingress.DATA_MESSAGES_THRESHOLD) {
            //
            // Determine if we should push the metadata or not
            //

            encoder.setClassId(GTSHelper.classId(ingress.classKey, encoder.getMetadata().getName()));
            encoder.setLabelsId(GTSHelper.labelsId(ingress.labelsKey, encoder.getMetadata().getLabels()));

            GTSHelper.fillGTSIds(bytes, 0, encoder.getClassId(), encoder.getLabelsId());

            BigInteger metadataCacheKey = new BigInteger(bytes);

            //
            // Check throttling
            //

            if (null != lastencoder && lastencoder.size() > 0) {
              ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId());
              ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount());
            }

            boolean pushMeta = false;

            Long lastActivity = ingress.metadataCache.getOrDefault(metadataCacheKey, ingress.NO_LAST_ACTIVITY);

            if (ingress.NO_LAST_ACTIVITY.equals(lastActivity)) {
              pushMeta = true;
            } else if (ingress.activityTracking && ingress.updateActivity) {
              if (null == lastActivity) {
                pushMeta = true;
              } else if (nowms - lastActivity > ingress.activityWindow) {
                pushMeta = true;
              }
            }

            if (pushMeta) {
              // Build metadata object to push
              Metadata metadata = new Metadata();
              // Set source to indicate we
              metadata.setSource(Configuration.INGRESS_METADATA_SOURCE);
              metadata.setName(encoder.getMetadata().getName());
              metadata.setLabels(encoder.getMetadata().getLabels());

              if (ingress.activityTracking && ingress.updateActivity) {
                metadata.setLastActivity(nowms);
              }

              TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
              try {
                ingress.pushMetadataMessage(bytes, serializer.serialize(metadata));

                // Update metadataCache with the current key
                synchronized (ingress.metadataCache) {
                  ingress.metadataCache.put(metadataCacheKey, (ingress.activityTracking && ingress.updateActivity) ? nowms : null);
                }
              } catch (TException te) {
                throw new IOException("Unable to push metadata.");
              }
            }

            if (null != lastencoder) {
              ingress.pushDataMessage(lastencoder, kafkaDataMessageAttributes);

              if (ingress.parseAttributes && lastHadAttributes) {
                // We need to push lastencoder's metadata update as they were updated since the last
                // metadata update message sent
                Metadata meta = new Metadata(lastencoder.getMetadata());
                if (deltaAttributes) {
                  meta.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);
                } else {
                  meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
                }
                ingress.pushMetadataMessage(meta);
                // Reset lastHadAttributes
                lastHadAttributes = false;
              }
            }

            if (encoder != lastencoder) {
              // This is the case when we just parsed either the first input line or one for a different
              // GTS than the previous one.
              lastencoder = encoder;
            } else {
              // This is the case when lastencoder and encoder are identical, but lastencoder was too big and needed
              // to be flushed

              //lastencoder = null;
              //
              // Allocate a new GTSEncoder and reuse Metadata so we can
              // correctly handle a continuation line if this is what occurs next
              //
              Metadata metadata = lastencoder.getMetadata();
              lastencoder = new GTSEncoder(0L);
              lastencoder.setMetadata(metadata);
            }
          }

          if (0 == count % 1000) {
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);
            Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_GLOBAL, Sensision.EMPTY_LABELS, count);
            count = 0;
          }
        } while (true);

        if (null != lastencoder && lastencoder.size() > 0) {
          ThrottlingManager.checkMADS(lastencoder.getMetadata(), producer, owner, application, lastencoder.getClassId(), lastencoder.getLabelsId());
          ThrottlingManager.checkDDP(lastencoder.getMetadata(), producer, owner, application, (int) lastencoder.getCount());

          ingress.pushDataMessage(lastencoder, kafkaDataMessageAttributes);

          if (ingress.parseAttributes && lastHadAttributes) {
            // Push a metadata UPDATE message so attributes are stored
            // Build metadata object to push
            Metadata meta = new Metadata(lastencoder.getMetadata());
            if (deltaAttributes) {
              meta.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);
            } else {
              meta.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
            }
            ingress.pushMetadataMessage(meta);
          }
        }
      } catch (WarpException we) {
        throw new IOException(we);
      } finally {
        //
        // Flush message buffers into Kafka
        //

        ingress.pushMetadataMessage(null, null);
        ingress.pushDataMessage(null, kafkaDataMessageAttributes);

        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_RAW, sensisionLabels, count);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_DATAPOINTS_GLOBAL, Sensision.EMPTY_LABELS, count);

        long micros = (System.nanoTime() - nano) / 1000L;

        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_TIME_US, sensisionLabels, micros);
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_UPDATE_TIME_US_GLOBAL, Sensision.EMPTY_LABELS, micros);
      }

      response.setStatus(HttpServletResponse.SC_OK);
    } catch (Throwable t) { // Catch everything else this handler could return 200 on a OOM exception
      if (!response.isCommitted()) {
        String prefix = "Error when updating data: ";
        String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
        return;
      }
    }
  }
}
