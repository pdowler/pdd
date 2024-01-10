/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2024.                            (c) 2024.
*  Government of Canada                 Gouvernement du Canada
*  National Research Council            Conseil national de recherches
*  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
*  All rights reserved                  Tous droits réservés
*
*  NRC disclaims any warranties,        Le CNRC dénie toute garantie
*  expressed, implied, or               énoncée, implicite ou légale,
*  statutory, of any kind with          de quelque nature que ce
*  respect to the software,             soit, concernant le logiciel,
*  including without limitation         y compris sans restriction
*  any warranty of merchantability      toute garantie de valeur
*  or fitness for a particular          marchande ou de pertinence
*  purpose. NRC shall not be            pour un usage particulier.
*  liable in any event for any          Le CNRC ne pourra en aucun cas
*  damages, whether direct or           être tenu responsable de tout
*  indirect, special or general,        dommage, direct ou indirect,
*  consequential or incidental,         particulier ou général,
*  arising from the use of the          accessoire ou fortuit, résultant
*  software.  Neither the name          de l'utilisation du logiciel. Ni
*  of the National Research             le nom du Conseil National de
*  Council of Canada nor the            Recherches du Canada ni les noms
*  names of its contributors may        de ses  participants ne peuvent
*  be used to endorse or promote        être utilisés pour approuver ou
*  products derived from this           promouvoir les produits dérivés
*  software without specific prior      de ce logiciel sans autorisation
*  written permission.                  préalable et particulière
*                                       par écrit.
*
*  This file is part of the             Ce fichier fait partie du projet
*  OpenCADC project.                    OpenCADC.
*
*  OpenCADC is free software:           OpenCADC est un logiciel libre ;
*  you can redistribute it and/or       vous pouvez le redistribuer ou le
*  modify it under the terms of         modifier suivant les termes de
*  the GNU Affero General Public        la “GNU Affero General Public
*  License as published by the          License” telle que publiée
*  Free Software Foundation,            par la Free Software Foundation
*  either version 3 of the              : soit la version 3 de cette
*  License, or (at your option)         licence, soit (à votre gré)
*  any later version.                   toute version ultérieure.
*
*  OpenCADC is distributed in the       OpenCADC est distribué
*  hope that it will be useful,         dans l’espoir qu’il vous
*  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
*  without even the implied             GARANTIE : sans même la garantie
*  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
*  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
*  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
*  General Public License for           Générale Publique GNU Affero
*  more details.                        pour plus de détails.
*
*  You should have received             Vous devriez avoir reçu une
*  a copy of the GNU Affero             copie de la Licence Générale
*  General Public License along         Publique GNU Affero avec
*  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
*  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
*                                       <http://www.gnu.org/licenses/>.
*
************************************************************************
*/

package org.opencadc.vault.migrate;

import ca.nrc.cadc.vos.VOSURI;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;
import org.opencadc.vault.NodePersistenceImpl;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeNotSupportedException;

/**
 * Job to migrate a single container node (recursive).
 * 
 * @author pdowler
 */
public class MigrateJob implements Runnable {
    private static final Logger log = Logger.getLogger(MigrateJob.class);

    private final SourceNodeIterator src;
    private final NodePersistenceImpl dest;
    private final ca.nrc.cadc.vos.ContainerNode node;
    
    boolean dryrun = false;
    
    public MigrateJob(SourceNodeIterator src, NodePersistenceImpl dest, ca.nrc.cadc.vos.ContainerNode node) {
        this.src = src;
        this.dest = dest;
        this.node = node;
    }

    @Override
    public void run() {
        NodeConvert conv = new NodeConvert(dest.getRootNode().getID());
        URI curURI = null;
        try {
            int num = 0;
            long start = System.currentTimeMillis();
            long pmin = Long.MAX_VALUE;
            long pmax = 0L;
            long ptotal = 0L;

            src.setContainer(node);
            LinkedBlockingQueue<ca.nrc.cadc.vos.Node> queue = new LinkedBlockingQueue<>(2000);
            NodeProducer producer = new NodeProducer(src, queue);
            final String fmt = "%d %s %s";
            boolean done = false;
            while (!done) {
                ca.nrc.cadc.vos.Node sn = queue.take(); // blocks
                curURI = sn.getUri().getURI();
                if (producer.terminate == sn) {
                    done = true;
                } else {
                    Node out = conv.convert(sn);
                    num++;
                    log.info(String.format(fmt, num, sn.getClass().getSimpleName(), sn.getUri().getPath()));
                    if (!dryrun) {
                        long t1 = System.nanoTime();
                        dest.put(out);
                        long pt = System.nanoTime() - t1;
                        ptotal += pt;
                        pmin = Math.min(pmin, pt);
                        pmax = Math.max(pmax, pt);
                    }
                }
            }
            log.info("summary " + node.getName() + " source-maxRecursionQueueSize: " + src.maxRecursionQueueSize);
            

            if (!dryrun) {
                final String putFmt = "summary %s %s: %.2f ms";
                log.info(String.format(putFmt, node.getName(), "min-put", ((double) pmin) / 1.0e6));
                log.info(String.format(putFmt, node.getName(), "max-put", ((double) pmax) / 1.0e6));
                log.info(String.format(putFmt, node.getName(), "avg-put", ((double) ptotal) / (num * 1.0e6)));
                long totalPut = ptotal / (1000L * 1000L); // ms
                long totalTime = System.currentTimeMillis() - start;
                long rate = (long) (1000L * num / totalTime);
                log.info(String.format("summary %s count: %d source-query: %d dest-put: %d total-time: %d ms rate: %d nodes/sec", 
                        node.getName(), num, src.timeQuerying, totalPut, totalTime, rate));
            } else {
                log.info("summary " + node.getName() + " source-timeQuerying: " + src.timeQuerying + "ms");
            }
        } catch (InterruptedException ex) {
            log.warn("MigrateWorker terminating: interrupt()");
        } catch (IllegalArgumentException | URISyntaxException ex) {
            log.error("FAIL bad content at " + curURI, ex);
        } catch (Exception ex) {
            log.error("FAIL unexpected at " + curURI, ex);
        }
    }

    private class NodeProducer implements Runnable {
        private Iterator<ca.nrc.cadc.vos.Node> inner;
        private final LinkedBlockingQueue<ca.nrc.cadc.vos.Node> queue;
        ca.nrc.cadc.vos.Node terminate = new TerminateNode();

        public NodeProducer(Iterator<ca.nrc.cadc.vos.Node> inner,LinkedBlockingQueue<ca.nrc.cadc.vos.Node> queue) {
            this.inner = inner;
            this.queue = queue;
            Thread bg = new Thread(this);
            bg.setDaemon(true);
            bg.start();
        }
        
        @Override
        public void run() {
            log.warn("NodeProducer.run() START");
            int num = 0;
            long t1 = System.currentTimeMillis();
            while (inner.hasNext()) {
                try {
                    queue.put(inner.next()); // block at capacity
                    num++;
                    long t2 = System.currentTimeMillis();
                    long dt = t2 - t1;
                    if (dt > 120 * 1000L) { // 2 min
                        int qs = queue.size();
                        log.info("NodeProducer.summary queueSize=" + qs + " num=" + num);
                        t1 = t2;
                    }
                } catch (InterruptedException ex) {
                    log.warn("NodeProducer.run() interrupted");
                } catch (Exception ex) {
                    log.error("NodeProducer.run() FAIL", ex);
                }
            }
            try {
                queue.put(terminate);
            } catch (InterruptedException ex) {
                log.warn("NodeProducer.run() terminate interrupted");
            } catch (Exception ex) {
                log.error("NodeProducer.run() terminate FAIL", ex);
            }
            log.warn("NodeProducer.run() DONE num=" + num);
        }
    }
    
    private class TerminateNode extends ca.nrc.cadc.vos.LinkNode {
        public TerminateNode() {
            super(new VOSURI(URI.create("vos://authority~service/terminate")), URI.create("vos:terminate"));
        }
    }
}
