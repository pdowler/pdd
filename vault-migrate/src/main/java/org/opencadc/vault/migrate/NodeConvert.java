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

import ca.nrc.cadc.vos.VOS;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.opencadc.gms.GroupURI;
import org.opencadc.vospace.ContainerNode;
import org.opencadc.vospace.DataNode;
import org.opencadc.vospace.LinkNode;
import org.opencadc.vospace.Node;
import org.opencadc.vospace.NodeProperty;

/**
 *
 * @author pdowler
 */
public class NodeConvert {
    private static final Logger log = Logger.getLogger(NodeConvert.class);

    // list of props with special handling that are ignored if seen in the node.properties
    private static List<String> IGNORE_PROPS = Arrays.asList(new String[] {
        VOS.PROPERTY_URI_AVAILABLESPACE,
        VOS.PROPERTY_URI_CONTENTENCODING,
        VOS.PROPERTY_URI_CONTENTLENGTH,
        VOS.PROPERTY_URI_CONTENTMD5,
        VOS.PROPERTY_URI_CREATION_DATE,
        VOS.PROPERTY_URI_CREATOR,
        VOS.PROPERTY_URI_DATE,
        VOS.PROPERTY_URI_FORMAT,
        VOS.PROPERTY_URI_GROUPMASK,
        VOS.PROPERTY_URI_GROUPREAD,
        VOS.PROPERTY_URI_GROUPWRITE,
        VOS.PROPERTY_URI_ISLOCKED,
        VOS.PROPERTY_URI_ISPUBLIC,
        VOS.PROPERTY_URI_QUOTA,
        VOS.PROPERTY_URI_READABLE,
        VOS.PROPERTY_URI_TYPE,
        VOS.PROPERTY_URI_WRITABLE
    });
    
    private final UUID rootID;
    
    public NodeConvert(UUID rootID) {
        this.rootID = rootID;
    }
    
    public Node convert(ca.nrc.cadc.vos.Node in) throws URISyntaxException {
        ca.nrc.cadc.vos.server.NodeID nid = (ca.nrc.cadc.vos.server.NodeID) in.appData;
        UUID id = new UUID(0L, nid.id);
            
        log.debug("in: " + in);
        if (in instanceof ca.nrc.cadc.vos.ContainerNode) {
            ContainerNode ret = new ContainerNode(id, in.getName());
            copyCommon(nid, in, ret);
            ret.inheritPermissions = true; // hardcoded behaviour
            return ret;
        }
        
        if (in instanceof ca.nrc.cadc.vos.DataNode) {
            DataNode ret = new DataNode(id, in.getName());
            copyCommon(nid, in, ret);
            ret.busy = false;
            ret.storageID = URI.create("cadc:vault/" + nid.storageID);
            return ret;
        }
        
        if (in instanceof ca.nrc.cadc.vos.LinkNode) {
            LinkNode ret = new LinkNode(id, in.getName(), ((ca.nrc.cadc.vos.LinkNode) in).getTarget());
            copyCommon(nid, in, ret);
            return ret;
        }
        
        throw new UnsupportedOperationException("convert " + in.getClass().getName());
    }
    
    private void copyCommon(ca.nrc.cadc.vos.server.NodeID nid, ca.nrc.cadc.vos.Node in, Node ret) throws URISyntaxException {
        ret.isLocked = in.isLocked();
        ret.isPublic = in.isPublic();
        
        ret.ownerID = nid.ownerObject; // ACIdentityManager specific behaviour
        ca.nrc.cadc.vos.server.NodeID pid = (ca.nrc.cadc.vos.server.NodeID) in.getParent().appData;
        if (pid != null && pid.id != null) {
            ret.parentID = new UUID(0L, pid.id);
        } else {
            ret.parentID = rootID;
        }
        
        String raw = in.getPropertyValue(ca.nrc.cadc.vos.VOS.PROPERTY_URI_GROUPREAD);
        if (raw != null) {
            String[] groups = raw.split(" ");
            for (String s : groups) {
                s = s.replace('#', '?');
                URI u = new URI(s);
                GroupURI g = new GroupURI(u);
                ret.getReadOnlyGroup().add(g);
            }
        }
        raw = in.getPropertyValue(ca.nrc.cadc.vos.VOS.PROPERTY_URI_GROUPWRITE);
        if (raw != null) {
            String[] groups = raw.split(" ");
            for (String s : groups) {
                s = s.replace('#', '?');
                URI u = new URI(s);
                GroupURI g = new GroupURI(u);
                ret.getReadWriteGroup().add(g);
            }
        }
        
        for (ca.nrc.cadc.vos.NodeProperty ip : in.getProperties()) {
            if (!IGNORE_PROPS.contains(ip.getPropertyURI())) {
                NodeProperty np = new NodeProperty(URI.create(ip.getPropertyURI()), ip.getPropertyValue());
                ret.getProperties().add(np);
            }
        }
    }
}
