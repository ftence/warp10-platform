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
import io.warp10.continuum.MetadataUtils;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class IngressMetaHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(IngressMetaHandler.class);

  private final Ingress ingress;

  public IngressMetaHandler(Ingress ingress) {
    this.ingress = ingress;
  }

  /**
   * Handle metadata updating
   */
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
    //
    // Only handle /meta
    //

    if (!target.equals(Constants.API_ENDPOINT_META)) {
      return;
    }

    //
    // Make the request as handled
    //

    baseRequest.setHandled(true);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_REQUESTS, Sensision.EMPTY_LABELS, 1);

    //
    // Check the endpoint is activated
    //

    if (null != WarpManager.getAttribute(WarpManager.META_DISABLED)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, String.valueOf(WarpManager.getAttribute(WarpManager.META_DISABLED)));
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

      String token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));

      WriteToken writeToken;

      try {
        writeToken = Tokens.extractWriteToken(token);
        if (writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_NOMETA)) {
          response.sendError(HttpServletResponse.SC_FORBIDDEN, "Token cannot be used for updating metadata.");
          return;
        }
      } catch (WarpScriptException ee) {
        throw new IOException(ee);
      }

      String application = writeToken.getAppName();
      String producer = Tokens.getUUID(writeToken.getProducerId());
      String owner = Tokens.getUUID(writeToken.getOwnerId());

      //
      // For meta operations, producer and owner MUST be non-null
      //

      if (null == producer || null == owner) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_INVALIDTOKEN, Sensision.EMPTY_LABELS, 1);
        response.sendError(HttpServletResponse.SC_FORBIDDEN, "Invalid token.");
        return;
      }

      //
      // Sensision labels (producer and application).
      //

      sensisionLabels = new HashMap<String, String>();
      sensisionLabels.put(SensisionConstants.SENSISION_LABEL_PRODUCER, producer);
      if (null != application) {
        sensisionLabels.put(SensisionConstants.SENSISION_LABEL_APPLICATION, application);
      }

      long count = 0;

      //
      // Determine if content if gzipped
      //

      boolean gzipped = false;

      if (null != request.getHeader("Content-Type") && "application/gzip".equals(request.getHeader("Content-Type"))) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_GZIPPED, sensisionLabels, 1);
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

        if (null == metadata) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_INVALID, sensisionLabels, 1);
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid metadata " + line);
          return;
        }

        // Add labels from the WriteToken if they exist
        if (writeToken.getLabelsSize() > 0) {
          metadata.getLabels().putAll(writeToken.getLabels());
        }

        //
        // Force owner/producer
        //

        metadata.getLabels().put(Constants.PRODUCER_LABEL, producer);
        metadata.getLabels().put(Constants.OWNER_LABEL, owner);

        // Force internal labels application
        // FIXME(hbs): remove me, apps should be set in all tokens now...
        if (null != application) {
          metadata.getLabels().put(Constants.APPLICATION_LABEL, application);
        } else {
          // remove application label
          metadata.getLabels().remove(Constants.APPLICATION_LABEL);
        }

        if (!MetadataUtils.validateMetadata(metadata)) {
          Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_INVALID, sensisionLabels, 1);
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid metadata " + line);
          return;
        }

        count++;

        if (deltaAttributes) {
          metadata.setSource(Configuration.INGRESS_METADATA_UPDATE_DELTA_ENDPOINT);
        } else {
          metadata.setSource(Configuration.INGRESS_METADATA_UPDATE_ENDPOINT);
        }

        try {
          // We do not take into consideration this activity timestamp in the cache
          // this way we do not allocate BigIntegers
          if (ingress.activityTracking && ingress.metaActivity) {
            metadata.setLastActivity(nowms);
          }
          ingress.pushMetadataMessage(metadata);
        } catch (Exception e) {
          throw new IOException("Unable to push metadata");
        }
      }

      response.setStatus(HttpServletResponse.SC_OK);
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_META_RECORDS, sensisionLabels, count);
    } catch (Throwable t) { // Catch everything else this handler could return 200 on a OOM exception
      if (!response.isCommitted()) {
        String prefix = "Error when updating meta: ";
        String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
        return;
      }
    } finally {
      //
      // Flush message buffers into Kafka
      //

      ingress.pushMetadataMessage(null, null);
    }
  }
}
