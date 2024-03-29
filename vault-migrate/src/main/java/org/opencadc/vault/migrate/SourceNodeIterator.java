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

import ca.nrc.cadc.date.DateUtil;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.vos.ContainerNode;
import ca.nrc.cadc.vos.Node;
import ca.nrc.cadc.vos.NodeProperty;
import ca.nrc.cadc.vos.VOSURI;
import ca.nrc.cadc.vos.server.db.DatabaseNodePersistence;
import ca.nrc.cadc.vospace.VOSpaceNodePersistence;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;

/**
 *
 * @author pdowler
 */
public class SourceNodeIterator implements Iterator<ca.nrc.cadc.vos.Node> {
    private static final Logger log = Logger.getLogger(SourceNodeIterator.class);

    private final DatabaseNodePersistence nodePer;
    
    private final LinkedList<Node> batch = new LinkedList<>();
    private final LinkedList<ContainerNode> recursionQueue = new LinkedList<>();
    private ContainerNode curParent;
    private Node curNode;
    private boolean lastBatchPartial;
    
    private final Map<Long,List<NodeProperty>> propMap;
    int maxRecursionQueueSize = 0;
    long timeQuerying = 0L;
    
    public SourceNodeIterator(DatabaseNodePersistence nodePer, Map<Long,List<NodeProperty>> propMap) {
        this.nodePer = nodePer;
        this.propMap = propMap;
        
        
        try {
            // hard coded incremental listing hack
            String startDate = null; // "2024-02-01T00:00:00.000";
            if (startDate != null) {
                DateFormat df = DateUtil.getDateFormat(DateUtil.IVOA_DATE_FORMAT, DateUtil.UTC);
                ca.nrc.cadc.vos.server.db.NodeDAO.INCREMENTAL_HACK = df.parse(startDate);
            }
        } catch (ParseException ex) {
            throw new RuntimeException("BUG: invalid hard-coded INCREMENTAL_HACK", ex);
        }
    }
    
    public void setContainer(ContainerNode curParent) {
        this.curParent = curParent;
        // start new base parent
        this.curNode = null;
        this.lastBatchPartial = false;
        batch.clear();          
        recursionQueue.clear();
        advance();
    }
    
    static final Map<Long,List<NodeProperty>> initPropMap() {
        try {
            DataSource ds = DBUtil.findJNDIDataSource(VOSpaceNodePersistence.DATASOURCE_NAME);
            String sql = "SELECT nodeID, propertyURI, propertyValue FROM NodeProperty order by nodeID";
            JdbcTemplate jdbc = new JdbcTemplate(ds);
            log.info("building NodeProperty cache ...");
            NPRowMapper npr = new NPRowMapper();
            Map<Long,List<NodeProperty>> ret = jdbc.query(sql, npr);
            log.info("NodeProperty cache: " + npr.numProps + " props for " + ret.size() + " distinct nodes");
            return ret;
        } catch (NamingException ex) {
            throw new RuntimeException("failed to find " + VOSpaceNodePersistence.DATASOURCE_NAME + " via JNDI");
        }
    }
    
    private static class NPRowMapper implements ResultSetExtractor<Map<Long,List<NodeProperty>>> {

        long numProps = 0L;
        
        @Override
        public Map<Long,List<NodeProperty>> extractData(ResultSet rs) throws SQLException, DataAccessException {
            Long id = null;
            List<NodeProperty> props = null;
            Map<Long,List<NodeProperty>> map = new HashMap<>();
            int numID = 0;
            while (rs.next()) {
                Long nid = rs.getLong(1);
                if (id == null || !id.equals(nid)) {
                    if (id != null && props != null && !props.isEmpty()) {
                        map.put(id, props);
                    }
                    // restart current
                    props = new ArrayList<>();
                    id = nid;
                }
                // add to current
                String uri = rs.getString(2);
                String val = rs.getString(3);
                props.add(new NodeProperty(uri, val));
                numProps++;
            }
            return map;
        }
    }
    
    @Override
    public boolean hasNext() {
        return curNode != null;
    }

    @Override
    public Node next() {
        if (curNode == null) {
            throw new NoSuchElementException();
        }
        Node ret = curNode;
        ca.nrc.cadc.vos.server.NodeID nid = (ca.nrc.cadc.vos.server.NodeID) ret.appData;
        List<NodeProperty> props = propMap.get(nid.id);
        if (props != null) {
            ret.getProperties().addAll(props);
        }
        advance();
        return ret;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
    
    // this impl avoids recusrion in favour of a loop
    private void advance() {
        boolean adv = advanceLoop();
        while (!adv) {
            adv = advanceLoop();
        }
    }
    
    // avoid recursion: return true when advanced
    private boolean advanceLoop() {
        // shift to next node in batch
        if (!batch.isEmpty()) {
            curNode = batch.pop();
            if (curNode instanceof ContainerNode) {
                log.debug("recursionQueue.push: " + curNode.getUri());
                recursionQueue.push((ContainerNode) curNode);
                maxRecursionQueueSize = Math.max(maxRecursionQueueSize, recursionQueue.size());
            }
            return true;
        }
        
        if (!lastBatchPartial) {
            // get next batch in curParent
            VOSURI vuri = null;
            if (curNode != null) {
                vuri = curNode.getUri();
            }
            
            log.debug("advance: query " + curParent.getUri() + " from " + vuri);
            long start = System.currentTimeMillis();
            nodePer.getChildren(curParent, vuri, 1000);
            this.timeQuerying += System.currentTimeMillis() - start;
            batch.addAll(curParent.getNodes());
            curParent.getNodes().clear();
            log.debug("advance: found " + batch.size());
            lastBatchPartial = (batch.size() < 1000);
            curNode = null;
            if (!batch.isEmpty()) {
                curNode = batch.pop();
                if (curNode.getUri().equals(vuri)) {
                    log.debug("advance: skip dupicate " + vuri);
                    curNode = null;
                    if (!batch.isEmpty()) {
                        curNode = batch.pop();
                    }
                }
            }
            if (curNode != null) {
                if (curNode instanceof ContainerNode) {
                    log.debug("recursionQueue.push: " + curNode.getUri());
                    recursionQueue.push((ContainerNode) curNode);
                    maxRecursionQueueSize = Math.max(maxRecursionQueueSize, recursionQueue.size());
                }
                return true;
            }
        } else {
            log.debug("avoided extra query for last node in container " + curParent.getUri());
            
        }
        
        // finished curParent
        lastBatchPartial = false;
        curParent = null;
        curNode = null;
        if (!recursionQueue.isEmpty()) {
            curParent = recursionQueue.pop();
            log.debug("recursionQueue.pop: " + curParent.getUri());
        }
        if (curParent != null) {
            // new parent but avoid recursion
            return false;
        }
        return true; // done
    }
}
