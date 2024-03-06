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

import ca.nrc.cadc.thread.ThreadedRunnableExecutor;
import ca.nrc.cadc.vos.server.db.DatabaseNodePersistence;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    private boolean deletions = false;
    private int threads = 1;
    
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

    public void setThreads(int threads) {
        this.threads = threads;
    }
    
    public void setDryrun(boolean dryrun) {
        this.dryrun = dryrun;
    }

    public void setDeletions(boolean deletions) {
        this.deletions = deletions;
    }

    @Override
    public Void run() throws Exception {
        if (deletions) {
            MigrateDeletionsTask job = new MigrateDeletionsTask(dest);
            job.dryrun = dryrun;
            job.run();
            return null;
        }

        // careful in root container
        ca.nrc.cadc.vos.ContainerNode srcRoot = (ca.nrc.cadc.vos.ContainerNode) src.get(SRCROOT);
        List<ca.nrc.cadc.vos.Node> targets = new ArrayList<>();
        if (nodes.isEmpty()) {
            src.getChildren(srcRoot);
            targets.addAll(srcRoot.getNodes());
        } else {
            for (String name : nodes) {
                String[] path = name.split("/");
                log.debug("path " + name + " length: " + path.length);
                ca.nrc.cadc.vos.ContainerNode cur = srcRoot;
                ca.nrc.cadc.vos.Node child = null;
                for (String n : path) {
                    if (!n.isEmpty()) { // leading / causes blank
                        src.getChild(cur, n);
                        if (cur.getNodes().isEmpty()) {
                            log.warn("not found: " + n + " from " + name);
                        } else {
                            child = cur.getNodes().get(0);
                            log.debug("found " + n + " = " + child.getUri());
                            if (child instanceof ca.nrc.cadc.vos.ContainerNode) {
                                cur = (ca.nrc.cadc.vos.ContainerNode) child;
                            }
                        }
                    }
                }
                log.info("target: " + name + " -> " + child.getClass().getSimpleName() + " " + child.getUri());
                targets.add(child);
                srcRoot.getNodes().clear();
            }
        }
        log.info("target nodes: " + targets.size());
        
        final LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
        int num = 0;
        Map<Long,List<ca.nrc.cadc.vos.NodeProperty>> propertyCache = null; // lazy init
        for (ca.nrc.cadc.vos.Node in : targets) {
            src.getProperties(in);
            Node nn = conv.convert(in);
            if (!dryrun) {
                dest.put(nn);
            }
            if (recursive && in instanceof ca.nrc.cadc.vos.ContainerNode) {
                if (propertyCache == null) {
                    propertyCache = SourceNodeIterator.initPropMap();
                }
                ca.nrc.cadc.vos.ContainerNode icn = (ca.nrc.cadc.vos.ContainerNode) in;
                SourceNodeIterator iter = new SourceNodeIterator(src, propertyCache);
                MigrateNodesTask job = new MigrateNodesTask(iter, dest, icn);
                job.dryrun = dryrun;
                queue.put(job);
            }
            // same log format as used in MigrateWorker
            log.info(String.format("%d %s %s", ++num, in.getClass().getSimpleName(), in.getUri().getPath()));
        }
        log.info("migrate jobs: " + queue.size());

        ThreadedRunnableExecutor threadPool = new ThreadedRunnableExecutor(queue, threads);
        log.info("migrate threads: " + threads);

        long poll = 12000L; // 2 rounds
        boolean waiting = true;
        while (waiting) {
            if (queue.isEmpty()) {
                // look more closely at state of thread pool
                if (threadPool.getAllThreadsIdle()) {
                    log.info("queue empty and threads idle - DONE");
                    waiting = false;
                } else {
                    log.debug("queue empty but threads working - Migrate.POLL dt=" + poll);
                    Thread.sleep(poll);
                }
            } else {
                log.debug("queue not empty - FileSync.POLL dt=" + poll);
                Thread.sleep(poll);
            }

        }
        threadPool.terminate();
        
        return null;
    }

    
}    
