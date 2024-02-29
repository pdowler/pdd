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
import ca.nrc.cadc.io.ResourceIterator;
import ca.nrc.cadc.vospace.VOSpaceNodePersistence;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import javax.naming.NamingException;
import javax.sql.DataSource;
import org.apache.log4j.Logger;
import org.opencadc.inventory.InventoryUtil;
import org.opencadc.inventory.db.Util;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.DeletedNodeEvent;
import org.opencadc.vospace.LinkNode;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;

/**
 *
 * @author pdowler
 */
public class SourceDeletedNodeEventIterator implements ResourceIterator<DeletedNodeEvent> {
    private static final Logger log = Logger.getLogger(SourceDeletedNodeEventIterator.class);

    private int batchSize = 1000;
    private Iterator<DeletedNodeEvent> batch;
    private Date curLastModified;
    private UUID curID;
    
    private final DNEMapper rowMapper = new DNEMapper();
    private final Calendar utc = Calendar.getInstance(DateUtil.UTC);
    
    public SourceDeletedNodeEventIterator(Date curLastModified, UUID curID) {
        this.curLastModified = curLastModified;
        this.curID = curID;
        advance();
    }

    @Override
    public boolean hasNext() {
        return (batch != null);
    }

    @Override
    public DeletedNodeEvent next() {
        DeletedNodeEvent ret = batch.next();
        curLastModified = ret.getLastModified();
        curID = ret.getID();
        advance();
        
        return ret;
    }
    
    private void advance() {
        // cal from ctor or next
        if (batch == null || !batch.hasNext()) {
            this.batch = null;
            List<DeletedNodeEvent> deleted = getBatch(curLastModified);
            if (!deleted.isEmpty()) {
                DeletedNodeEvent dne = deleted.get(0);
                this.batch = deleted.iterator();
                if (dne.getID().equals(curID)) {
                    batch.next(); // skip duplicate
                }
                if (!batch.hasNext()) {
                    batch = null; // done
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        // no-op since we are batching instead
    }

    private List<DeletedNodeEvent> getBatch(Date minLastModified) {
        try {
            DataSource ds = DBUtil.findJNDIDataSource(VOSpaceNodePersistence.DATASOURCE_NAME);
            JdbcTemplate jdbc = new JdbcTemplate(ds);
            BatchStatement batch = new BatchStatement();
            batch.minLastModified = minLastModified;
            List<DeletedNodeEvent> ret = jdbc.query(batch, rowMapper);
            return ret;
        } catch (NamingException ex) {
            throw new RuntimeException("failed to find " + VOSpaceNodePersistence.DATASOURCE_NAME + " via JNDI");
        }
    }
    
    private class BatchStatement implements PreparedStatementCreator {
        Date minLastModified;
        
        @Override
        public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT TOP ").append(batchSize);
            sb.append(" nodeID, nodeType, bucket, lastModified FROM DeletedNodeEvent");
            if (minLastModified != null) {
                sb.append(" WHERE lastModified >= ?");
            }
            sb.append(" ORDER BY  lastModified");
            String sql = sb.toString();
            log.debug("SQL: " + sql);
            PreparedStatement prep = conn.prepareStatement(sql);
            if (minLastModified != null) {
                prep.setTimestamp(1, new Timestamp(minLastModified.getTime()), utc);
            }
            
            return prep;
        }
        
    }

    private class DNEMapper implements RowMapper<DeletedNodeEvent> {

        @Override
        public DeletedNodeEvent mapRow(ResultSet rs, int i) throws SQLException {
            int col = 1;
            Long nodeID = rs.getLong(col++);
            String nodeType = rs.getString(col++);
            String bucket = rs.getString(col++);
            Date lastModified = Util.getDate(rs, col++, utc);
            
            char nt = nodeType.charAt(0);
            Class c = null;
            switch (nt) {
                case 'C':
                    c = ContainerNode.class;
                    break;
                case 'D':
                    c = DataNode.class;
                    break;
                case 'L':
                    c = LinkNode.class;
                    break;
                default:
                    throw new RuntimeException("unexpected nodeType: " + nt);
            }
            UUID id = new UUID(0L, nodeID);
            DeletedNodeEvent dae = new DeletedNodeEvent(id, c);
            InventoryUtil.assignLastModified(dae, lastModified);
            return dae;
        }
        
    }
}
