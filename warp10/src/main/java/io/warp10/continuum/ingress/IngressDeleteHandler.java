//
//   Copyright 2019  SenX S.A.S.
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

import com.fasterxml.sort.SortConfig;
import io.warp10.ThrowableUtils;
import io.warp10.WarpManager;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.TextFileShuffler;
import io.warp10.continuum.Tokens;
import io.warp10.continuum.egress.EgressFetchHandler;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.sensision.SensisionConstants;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.MetadataIterator;
import io.warp10.continuum.store.thrift.data.DirectoryRequest;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.crypto.OrderPreservingBase64;
import io.warp10.quasar.token.thrift.data.WriteToken;
import io.warp10.script.WarpScriptException;
import io.warp10.sensision.Sensision;
import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.apache.thrift.TDeserializer;
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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;

public class IngressDeleteHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(IngressDeleteHandler.class);

  private final Ingress ingress;

  public IngressDeleteHandler(Ingress ingress) {
    this.ingress = ingress;
  }

  /**
   * Handle metadata and data deletion
   */
  @Override
  public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
    //
    // Only handle /delete
    //

    if (!target.equals(Constants.API_ENDPOINT_DELETE)) {
      return;
    }

    //
    // Make the request as handled
    //

    baseRequest.setHandled(true);
    Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_DELETE_REQUESTS, Sensision.EMPTY_LABELS, 1);

    //
    // Check the endpoint is activated
    //

    if (ingress.rejectDelete) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, Constants.API_ENDPOINT_DELETE + " endpoint is not activated.");
      return;
    }

    if (null != WarpManager.getAttribute(WarpManager.DELETE_DISABLED)) {
      response.sendError(HttpServletResponse.SC_FORBIDDEN, String.valueOf(WarpManager.getAttribute(WarpManager.DELETE_DISABLED)));
      return;
    }

    long gts = 0;

    boolean completeDeletion = false;

    boolean dryrun = null != request.getParameter(Constants.HTTP_PARAM_DRYRUN);

    boolean showErrors = null != request.getParameter(Constants.HTTP_PARAM_SHOW_ERRORS);

    PrintWriter pw = null;

    // By default, no label, will be updated later.
    Map<String, String> sensisionLabels = Sensision.EMPTY_LABELS;

    try {
      //
      // CORS header
      //

      response.setHeader("Access-Control-Allow-Origin", "*");

      //
      // Extract token infos
      //

      String token = request.getHeader(Constants.getHeader(Configuration.HTTP_HEADER_TOKENX));

      WriteToken writeToken;

      try {
        writeToken = Tokens.extractWriteToken(token);
        if (writeToken.getAttributesSize() > 0 && writeToken.getAttributes().containsKey(Constants.TOKEN_ATTR_NODELETE)) {
          response.sendError(HttpServletResponse.SC_FORBIDDEN, "Token cannot be used for deletions.");
          return;
        }
      } catch (WarpScriptException ee) {
        throw new IOException(ee);
      }

      String application = writeToken.getAppName();
      String producer = Tokens.getUUID(writeToken.getProducerId());
      String owner = Tokens.getUUID(writeToken.getOwnerId());

      //
      // For delete operations, producer and owner MUST be equal and non-null
      //

      if (null == producer || !producer.equals(owner)) {
        Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_DELETE_INVALIDTOKEN, Sensision.EMPTY_LABELS, 1);
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

      //
      // Build extra labels
      //

      Map<String, String> extraLabels = new HashMap<String, String>();

      // Add extra labels, remove producer,owner,app
      if (writeToken.getLabelsSize() > 0) {
        extraLabels.putAll(writeToken.getLabels());
        extraLabels.remove(Constants.PRODUCER_LABEL);
      }

      // Only set owner, producer may vary
      extraLabels.put(Constants.OWNER_LABEL, owner);

      // Force internal labels application
      // FIXME(hbs): remove me, apps should be set in all tokens now...
      if (null != application) {
        extraLabels.put(Constants.APPLICATION_LABEL, application);
      } else {
        // remove application label
        extraLabels.remove(Constants.APPLICATION_LABEL);
      }

      //
      // Extract start/end
      //

      String startstr = request.getParameter(Constants.HTTP_PARAM_START);
      String endstr = request.getParameter(Constants.HTTP_PARAM_END);

      String minagestr = request.getParameter(Constants.HTTP_PARAM_MINAGE);

      long start = Long.MIN_VALUE;
      long end = Long.MAX_VALUE;

      long minage = 0L;

      if (null != minagestr) {
        minage = Long.parseLong(minagestr);

        if (minage < 0) {
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid value for '" + Constants.HTTP_PARAM_MINAGE + "', expected a number of ms >= 0");
          return;
        }
      }

      if (null != startstr) {
        if (null == endstr) {
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Both " + Constants.HTTP_PARAM_START + " and " + Constants.HTTP_PARAM_END + " should be defined.");
          return;
        }
        if (startstr.contains("T")) {
          if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_1_8)) {
            start = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(startstr);
          } else {
            start = ingress.fmt.parseDateTime(startstr).getMillis() * Constants.TIME_UNITS_PER_MS;
          }
        } else {
          start = Long.valueOf(startstr);
        }
      }

      if (null != endstr) {
        if (null == startstr) {
          response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Both " + Constants.HTTP_PARAM_START + " and " + Constants.HTTP_PARAM_END + " should be defined.");
          return;
        }
        if (endstr.contains("T")) {
          if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_1_8)) {
            end = io.warp10.script.unary.TOTIMESTAMP.parseTimestamp(endstr);
          } else {
            end = ingress.fmt.parseDateTime(endstr).getMillis() * Constants.TIME_UNITS_PER_MS;
          }
        } else {
          end = Long.valueOf(endstr);
        }
      }

      if (Long.MIN_VALUE == start && Long.MAX_VALUE == end && null == request.getParameter(Constants.HTTP_PARAM_DELETEALL)) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Parameter " + Constants.HTTP_PARAM_DELETEALL + " should be set when deleting a full range.");
        return;
      }

      if (start > end) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST, "Invalid time range specification.");
        return;
      }

      //
      // Extract selector
      //

      String selector = request.getParameter(Constants.HTTP_PARAM_SELECTOR);

      //
      // Extract the class and labels selectors
      // The class selector and label selectors are supposed to have
      // values which use percent encoding, i.e. explicit percent encoding which
      // might have been re-encoded using percent encoding when passed as parameter.
      //

      Matcher m = EgressFetchHandler.SELECTOR_RE.matcher(selector);

      if (!m.matches()) {
        response.sendError(HttpServletResponse.SC_BAD_REQUEST);
        return;
      }

      String classSelector = URLDecoder.decode(m.group(1), StandardCharsets.UTF_8.name());
      String labelsSelection = m.group(2);

      Map<String, String> labelsSelectors;

      try {
        labelsSelectors = GTSHelper.parseLabelsSelectors(labelsSelection);
      } catch (ParseException pe) {
        throw new IOException(pe);
      }

      //
      // Force 'owner'/'app' from token
      //

      labelsSelectors.putAll(extraLabels);

      List<String> clsSels = new ArrayList<String>();
      List<Map<String, String>> lblsSels = new ArrayList<Map<String, String>>();

      clsSels.add(classSelector);
      lblsSels.add(labelsSelectors);


      response.setStatus(HttpServletResponse.SC_OK);

      pw = response.getWriter();
      StringBuilder sb = new StringBuilder();

      //
      // Shuffle only if not in dryrun mode
      //

      if (!dryrun && ingress.doShuffle) {
        //
        // Loop over the iterators, storing the read metadata to a temporary file encrypted on disk
        // Data is encrypted using a onetime pad
        //

        final byte[] onetimepad = new byte[(int) Math.max(65537, System.currentTimeMillis() % 100000)];
        new Random().nextBytes(onetimepad);

        final File cache = File.createTempFile(Long.toHexString(System.currentTimeMillis()) + "-" + Long.toHexString(System.nanoTime()), ".delete.dircache");
        cache.deleteOnExit();

        FileWriter writer = new FileWriter(cache);

        TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());

        DirectoryRequest drequest = new DirectoryRequest();
        drequest.setClassSelectors(clsSels);
        drequest.setLabelsSelectors(lblsSels);

        try (MetadataIterator iterator = ingress.directoryClient.iterator(drequest)) {
          while (iterator.hasNext()) {
            Metadata metadata = iterator.next();

            try {
              byte[] bytes = serializer.serialize(metadata);
              // Apply onetimepad
              // We pad each line separately since we will later shuffle them!
              int padidx = 0;

              for (int i = 0; i < bytes.length; i++) {
                bytes[i] = (byte) (bytes[i] ^ onetimepad[padidx++]);
                if (padidx >= onetimepad.length) {
                  padidx = 0;
                }
              }
              OrderPreservingBase64.encodeToWriter(bytes, writer);
              writer.write('\n');
            } catch (TException te) {
              throw new IOException(te);
            }
          }
        } catch (Exception e) {
          try {
            writer.close();
          } catch (IOException ioe) {
          }
          cache.delete();
          throw new IOException(e);
        }

        writer.close();

        //
        // Shuffle the content of the file
        //

        final File shuffled = File.createTempFile(Long.toHexString(System.currentTimeMillis()) + "-" + Long.toHexString(System.nanoTime()), ".delete.shuffled");
        shuffled.deleteOnExit();

        TextFileShuffler shuffler = new TextFileShuffler(new SortConfig().withMaxMemoryUsage(1000000L));

        InputStream in = new FileInputStream(cache);
        OutputStream out = new FileOutputStream(shuffled);

        try {
          shuffler.sort(in, out);
        } catch (Exception e) {
          try {
            in.close();
          } catch (IOException ioe) {
          }
          try {
            out.close();
          } catch (IOException ioe) {
          }
          shuffler.close();
          shuffled.delete();
          cache.delete();
          throw new IOException(e);
        }

        shuffler.close();
        out.close();
        in.close();

        // Delete the unshuffled file
        cache.delete();

        //
        // Create an iterator based on the shuffled cache
        //

        final AtomicReference<Throwable> error = new AtomicReference<Throwable>(null);

        MetadataIterator shufflediterator = new MetadataIterator() {

          BufferedReader reader = new BufferedReader(new FileReader(shuffled));

          private Metadata current = null;
          private boolean done = false;

          private TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

          @Override
          public boolean hasNext() {
            if (done) {
              return false;
            }

            if (null != current) {
              return true;
            }

            try {
              String line = reader.readLine();
              if (null == line) {
                done = true;
                return false;
              }
              byte[] raw = OrderPreservingBase64.decode(line.getBytes(StandardCharsets.US_ASCII));
              // Apply one time pad
              int padidx = 0;

              for (int i = 0; i < raw.length; i++) {
                raw[i] = (byte) (raw[i] ^ onetimepad[padidx++]);
                if (padidx >= onetimepad.length) {
                  padidx = 0;
                }
              }
              Metadata metadata = new Metadata();
              try {
                deserializer.deserialize(metadata, raw);
                this.current = metadata;
                return true;
              } catch (TException te) {
                error.set(te);
                LOG.error("", te);
              }
            } catch (IOException ioe) {
              error.set(ioe);
              LOG.error("", ioe);
            }

            return false;
          }

          @Override
          public Metadata next() {
            if (null != this.current) {
              Metadata metadata = this.current;
              this.current = null;
              return metadata;
            } else {
              throw new NoSuchElementException();
            }
          }

          @Override
          public void close() throws Exception {
            this.reader.close();
            shuffled.delete();
          }
        };

        try {
          while (shufflediterator.hasNext()) {
            Metadata metadata = shufflediterator.next();

            if (!dryrun) {
              ingress.pushDeleteMessage(start, end, minage, metadata);

              if (Long.MAX_VALUE == end && Long.MIN_VALUE == start && 0 == minage) {
                completeDeletion = true;
                // We must also push the metadata deletion and remove the metadata from the cache
                Metadata meta = new Metadata(metadata);
                meta.setSource(Configuration.INGRESS_METADATA_DELETE_SOURCE);
                ingress.pushMetadataMessage(meta);
                byte[] bytes = new byte[16];
                // We know class/labels Id were computed in pushMetadataMessage
                GTSHelper.fillGTSIds(bytes, 0, meta.getClassId(), meta.getLabelsId());
                BigInteger key = new BigInteger(bytes);
                synchronized (ingress.metadataCache) {
                  ingress.metadataCache.remove(key);
                }
              }
            }

            sb.setLength(0);

            GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels());

            if (metadata.getAttributesSize() > 0) {
              GTSHelper.labelsToString(sb, metadata.getAttributes());
            } else {
              sb.append("{}");
            }

            pw.write(sb.toString());
            pw.write("\r\n");
            gts++;
          }

          if (null != error.get()) {
            throw error.get();
          }
        } finally {
          try {
            shufflediterator.close();
          } catch (Exception e) {
          }
        }
      } else {

        DirectoryRequest drequest = new DirectoryRequest();
        drequest.setClassSelectors(clsSels);
        drequest.setLabelsSelectors(lblsSels);

        try (MetadataIterator iterator = ingress.directoryClient.iterator(drequest)) {
          while (iterator.hasNext()) {
            Metadata metadata = iterator.next();

            if (!dryrun) {
              ingress.pushDeleteMessage(start, end, minage, metadata);

              if (Long.MAX_VALUE == end && Long.MIN_VALUE == start && 0 == minage) {
                completeDeletion = true;
                // We must also push the metadata deletion and remove the metadata from the cache
                Metadata meta = new Metadata(metadata);
                meta.setSource(Configuration.INGRESS_METADATA_DELETE_SOURCE);
                ingress.pushMetadataMessage(meta);
                byte[] bytes = new byte[16];
                // We know class/labels Id were computed in pushMetadataMessage
                GTSHelper.fillGTSIds(bytes, 0, meta.getClassId(), meta.getLabelsId());
                BigInteger key = new BigInteger(bytes);
                synchronized (ingress.metadataCache) {
                  ingress.metadataCache.remove(key);
                }
              }
            }

            sb.setLength(0);

            GTSHelper.metadataToString(sb, metadata.getName(), metadata.getLabels());

            if (metadata.getAttributesSize() > 0) {
              GTSHelper.labelsToString(sb, metadata.getAttributes());
            } else {
              sb.append("{}");
            }

            pw.write(sb.toString());
            pw.write("\r\n");
            gts++;
          }
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
    } catch (Throwable t) {
      LOG.error("", t);
      Sensision.update(SensisionConstants.CLASS_WARP_INGRESS_DELETE_ERRORS, Sensision.EMPTY_LABELS, 1);
      if (showErrors && null != pw) {
        pw.println();
        StringWriter sw = new StringWriter();
        PrintWriter pw2 = new PrintWriter(sw);
        t.printStackTrace(pw2);
        pw2.close();
        sw.flush();
        String error = URLEncoder.encode(sw.toString(), StandardCharsets.UTF_8.name());
        pw.println(Constants.INGRESS_DELETE_ERROR_PREFIX + error);
      }
      if (!response.isCommitted()) {
        String prefix = "Error when deleting data: ";
        String msg = prefix + ThrowableUtils.getErrorMessage(t, Constants.MAX_HTTP_REASON_LENGTH - prefix.length());
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, msg);
      }
      return;
    } finally {
      // Flush delete messages
      if (!dryrun) {
        ingress.pushDeleteMessage(0L, 0L, 0L, null);
        if (completeDeletion) {
          ingress.pushMetadataMessage(null, null);
        }
      }
      Sensision.update(SensisionConstants.SENSISION_CLASS_CONTINUUM_INGRESS_DELETE_GTS, sensisionLabels, gts);
    }

    response.setStatus(HttpServletResponse.SC_OK);
  }
}
