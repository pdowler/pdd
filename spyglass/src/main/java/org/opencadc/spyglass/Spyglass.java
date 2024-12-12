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

package org.opencadc.spyglass;

import ca.nrc.cadc.auth.AuthMethod;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.net.FileContent;
import ca.nrc.cadc.net.HttpGet;
import ca.nrc.cadc.net.HttpPost;
import ca.nrc.cadc.net.ResourceNotFoundException;
import ca.nrc.cadc.reg.Capabilities;
import ca.nrc.cadc.reg.Capability;
import ca.nrc.cadc.reg.Interface;
import ca.nrc.cadc.reg.Standards;
import ca.nrc.cadc.reg.client.RegistryClient;
import ca.nrc.cadc.util.StringUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import org.apache.log4j.Logger;
import org.opencadc.vospace.VOS;
import org.opencadc.vospace.View;
import org.opencadc.vospace.transfer.Direction;
import org.opencadc.vospace.transfer.Protocol;
import org.opencadc.vospace.transfer.Transfer;
import org.opencadc.vospace.transfer.TransferReader;
import org.opencadc.vospace.transfer.TransferWriter;

/**
 *
 * @author pdowler
 */
public class Spyglass implements PrivilegedExceptionAction<Void> {
    private static final Logger log = Logger.getLogger(Spyglass.class);

    private final URI resourceID;
    private final URI target;
    private final Direction direction;
    
    // negotiation options
    public URI securityMethod;
    public URI standardID;
    public URI view;
    
    public Spyglass(URI resourceID, URI target, Direction direction) {
        this.resourceID = resourceID;
        this.target = target;
        this.direction = direction;
    }

    @Override
    public Void run() throws Exception {
        doit();
        return null;
    }
    
    public void doit() throws Exception {
        final AuthMethod am = AuthenticationUtil.getAuthMethodFromCredentials(AuthenticationUtil.getCurrentSubject());
        final URI sec = Standards.getSecurityMethod(am);
        
        long t1 = System.currentTimeMillis();
        URL turl = getTN(sec);
        log.info("transfer negotiation: " + turl);
        
        Transfer trans = negotiate(turl);
        long dt = System.currentTimeMillis() - t1;
        
        log.info("found " + trans.getProtocols().size() + " " + trans.getDirection().getValue() + " URLs in " + dt + "ms");
        for (Protocol p : trans.getProtocols()) {
            System.out.println();
            System.out.print(p.getEndpoint());
            URI sm = p.getSecurityMethod();
            if (sm == null) {
                System.out.println(" [anon]");
            } else {
                System.out.println(" [" + sm.toASCIIString() + "]");
            }
        }
    }
    
    private URL getTN(URI securityMethod) {
        try {
            // find transfer negotiation endpoint
            RegistryClient reg = new RegistryClient();
            log.info("capabilities: " + reg.getAccessURL(resourceID));
            
            Capabilities caps = reg.getCapabilities(resourceID);
            Capability cap = caps.findCapability(Standards.VOSPACE_SYNC_21);
            if (cap == null) {
                cap = caps.findCapability(Standards.SI_LOCATE);
            }
            if (cap == null) {
                throw new RuntimeException("resource " + resourceID + " does not implement a transfer negotiation API");
            }

            Interface iface = cap.findInterface(securityMethod);
            if (iface == null) {
                throw new RuntimeException("resource " + resourceID + " cap " + cap.getStandardID() + " does not support securityMethod " + securityMethod);
            }
            URL turl = iface.getAccessURL().getURL();
            return turl;
        } catch (IOException | ResourceNotFoundException ex) {
            throw new RuntimeException("failed to get capabilities for " +  resourceID, ex);
        }
    }
    
    private Transfer negotiate(URL turl) throws Exception {
        
        Transfer request = new Transfer(target, direction);
        request.version = VOS.VOSPACE_21;
        request.setView(new View(view));
        
        URI proto = VOS.PROTOCOL_HTTPS_GET;
        if (Direction.pushToVoSpace.equals(direction)) {
            proto = VOS.PROTOCOL_HTTPS_PUT;
        } else if (Direction.BIDIRECTIONAL.equals(direction)) {
            proto = VOS.PROTOCOL_SSHFS;
        }
        
        URI[] sms = new URI[] {
            null,
            Standards.SECURITY_METHOD_CERT,
            Standards.SECURITY_METHOD_TOKEN
        };
        
        if (securityMethod == null) {
            for (URI sm : sms) {
                Protocol p = new Protocol(proto);
                p.setSecurityMethod(sm);
                request.getProtocols().add(p);
            }
        } else {
            // specified
            Protocol p = new Protocol(proto);
            p.setSecurityMethod(securityMethod);
            request.getProtocols().add(p);
        }
        
        if (standardID != null) {
            Protocol p = new Protocol(standardID);
            p.setSecurityMethod(securityMethod);
            request.getProtocols().add(p);
        }
        
        TransferWriter writer = new TransferWriter();
        StringWriter out = new StringWriter();
        writer.write(request, out);
        String req = out.toString();
        log.info("request:\n" + req);

        FileContent content = new FileContent(req, "text/xml", Charset.forName("UTF-8"));
        HttpPost post = new HttpPost(turl, content, false);
        post.prepare();
        InputStream istream;

        if (post.getRedirectURL() != null) {
            HttpGet get = new HttpGet(post.getRedirectURL(), true);
            get.prepare();
            istream = get.getInputStream();
        } else {
            istream = post.getInputStream();
        }
        String xml = StringUtil.readFromInputStream(istream, "UTF-8");
        log.info("response:\n" + xml);

        TransferReader reader = new TransferReader();
        Transfer t = reader.read(xml, null);
        log.debug("Response transfer: " + t);
        return t;
    }
}
