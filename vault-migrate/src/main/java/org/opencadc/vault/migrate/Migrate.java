/*
************************************************************************
*******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
**************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
*
*  (c) 2023.                            (c) 2023.
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

import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.db.DatabaseNodePersistence;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;
import org.opencadc.vault.NodePersistenceImpl;
import org.opencadc.vospace.Node;

/**
 *
 * @author pdowler
 */
public class Migrate implements PrivilegedExceptionAction<Void> {
    private static final Logger log = Logger.getLogger(Migrate.class);

    private static final ca.nrc.cadc.vos.VOSURI SRCROOT = new ca.nrc.cadc.vos.VOSURI(URI.create("vos://cadc.nrc.ca!vault/"));
    
    private final DatabaseNodePersistence src;
    private final NodePersistenceImpl dest;
    private final NodeConvert conv;
    
    private final List<String> nodes = new ArrayList<>();
    private boolean recursive = false;
    private boolean dryrun = false;
    
    public Migrate(DatabaseNodePersistence src, NodePersistenceImpl dest) {
        this.src = src;
        this.dest = dest;
        this.conv = new NodeConvert(dest.getRootNode().getID());
    }

    public void setNodes(List<String> nodes) {
        this.nodes.addAll(nodes);
    }
    
    public void setRecursive(boolean r) {
        this.recursive = r;
    }

    public void setDryrun(boolean dryrun) {
        this.dryrun = dryrun;
    }

    @Override
    public Void run() throws Exception {
        // careful in root container
        ca.nrc.cadc.vos.ContainerNode srcRoot = (ca.nrc.cadc.vos.ContainerNode) src.get(SRCROOT);
        if (nodes.isEmpty()) {
            src.getChildren(srcRoot);
        } else {
            for (String name : nodes) {
                src.getChild(srcRoot, name);
            }
        }
        
        long num = 0;
        SourceNodeIterator iter = null;
        if (recursive) {
            iter = new SourceNodeIterator(src);
        }
        long start = System.currentTimeMillis();
        long blocked = 0L;
        long pmin = Long.MAX_VALUE;
        long pmax = 0L;
        long ptotal = 0L;
        for (ca.nrc.cadc.vos.Node in : srcRoot.getNodes()) {
            src.getProperties(in);
            num++;
            System.out.println(num + " " + in.getClass().getSimpleName() + " " + in.getUri().getPath());
            Node nn = conv.convert(in);
            if (!dryrun) {
                long t1 = System.nanoTime();
                dest.put(nn);
                long pt = System.nanoTime() - t1;
                ptotal += pt;
                pmin = Math.min(pmin, pt);
                pmax = Math.max(pmax, pt);
            }
            
            if (recursive && in instanceof ca.nrc.cadc.vos.ContainerNode) {
                ca.nrc.cadc.vos.ContainerNode icn = (ca.nrc.cadc.vos.ContainerNode) in;
                iter.setContainer(icn);
                LinkedBlockingQueue<ca.nrc.cadc.vos.Node> queue = new LinkedBlockingQueue<>(2000);
                NodeProducer producer = new NodeProducer(iter, queue);
                final String fmt = "%d %s %s";
                boolean done = false;
                while (!done) {
                    long b1 = System.nanoTime();
                    ca.nrc.cadc.vos.Node sn = queue.take(); // blocks
                    blocked += System.nanoTime() - b1;
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
                log.info("source-maxRecursionQueueSize: " + iter.maxRecursionQueueSize);
                log.info("source-timeQuerying: " + iter.timeQuerying + "ms");

                final String blockFmt = "queue-take: %.2f ms";
                log.info(String.format(blockFmt, ((double) blocked) / 1.0e6));

                if (!dryrun) {
                    final String putFmt = dest.getClass().getSimpleName() + ".%s: %.2f ms";
                    log.info(String.format(putFmt, "min-put", ((double) pmin) / 1.0e6));
                    log.info(String.format(putFmt, "max-put", ((double) pmax) / 1.0e6));
                    log.info(String.format(putFmt, "total-put", ((double) ptotal) / 1.0e6));
                }
            }
        }
        long dt = System.currentTimeMillis() - start;
        long tpn = dt / num;
        long sec = dt / 1000L;
        long rate = 1000 / tpn;
        log.info("migrated " + num + " nodes in " + dt + "ms (" + sec + "sec) aka ~" + rate + " nodes/sec");
        
        return null;
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
            log.warn("ThreadedNodeIterator.run() START");
            int num = 0;
            while (inner.hasNext()) {
                try {
                    queue.put(inner.next()); // block at capacity
                    num++;
                } catch (InterruptedException ex) {
                    log.warn("ThreadedNodeIterator.run() interrupted");
                }
            }
            try {
                queue.put(terminate);
            } catch (InterruptedException ex) {
                log.warn("ThreadedNodeIterator.run() interrupted");
            }
            log.warn("ThreadedNodeIterator.run() DONE num=" + num);
        }
    }
    
    private class TerminateNode extends ca.nrc.cadc.vos.LinkNode {
        public TerminateNode() {
            super(new VOSURI(URI.create("vos://authority~service/terminate")), URI.create("vos:terminate"));
        }
    }
}    
