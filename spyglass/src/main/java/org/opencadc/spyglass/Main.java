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

import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.AuthorizationToken;
import ca.nrc.cadc.auth.CertCmdArgUtil;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import javax.security.auth.Subject;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.vospace.transfer.Direction;

/**
 *
 * @author pdowler
 */
public class Main {
    private static final Logger log = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            ArgumentMap am = new ArgumentMap(args);
            Log4jInit.setLevel("org.opencadc", Level.WARN);
            Log4jInit.setLevel("ca.nrc.cadc", Level.WARN);
            if (am.isSet("v") || am.isSet("verbose")) {
                Log4jInit.setLevel("org.opencadc.spyglass", Level.INFO);
            } else if (am.isSet("d") || am.isSet("debug")) {
                Log4jInit.setLevel("org.opencadc.spyglass", Level.DEBUG);
                Log4jInit.setLevel("org.opencadc.vospace", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.reg", Level.DEBUG);
                Log4jInit.setLevel("ca.nrc.cadc.net", Level.DEBUG);
            } 
                
            boolean help = am.isSet("h") || am.isSet("help");
            String r = am.getValue("resourceID");
            List<String> pa = am.getPositionalArgs();
            if (help || r == null || pa.size() != 2) {
                usage();
                System.exit(0);
            }
            String d = pa.get(0);
            String t = pa.get(1);
            String secStr = am.getValue("securityMethod");
            String sidStr = am.getValue("protocol");
            String viewStr = am.getValue("view");
                        
            final Direction direction;
            if (Direction.pullFromVoSpace.getValue().equals(d)) {
                direction = Direction.pullFromVoSpace;
            } else if (Direction.pushToVoSpace.getValue().equals(d)) {
                direction = Direction.pushToVoSpace;
            } else if (Direction.BIDIRECTIONAL.getValue().equals(d)) {
                direction = Direction.BIDIRECTIONAL;
            } else {
                throw new IllegalArgumentException("invalid direction: " + d);
            }
            
            final URI resourceID = new URI(r);
            final URI target = new URI(t);
            URI sec = null;
            if (secStr != null) {
                sec = new URI(secStr);
            }
            URI sid = null;
            if (sidStr != null) {
                sid = new URI(sidStr);
            }
            URI view = null;
            if (viewStr != null) {
                view = new URI(viewStr);
            }
            
            Subject subject = AuthenticationUtil.getAnonSubject();
            if (am.isSet("cert")) {
                subject = CertCmdArgUtil.initSubject(am);
            } else if (am.isSet("token")) {
                subject = getSubjectWithToken(am.getValue("token"), am.getValue("domain"));
            }
            
            Spyglass spy = new Spyglass(resourceID, target, direction);
            spy.securityMethod = sec;
            spy.standardID = sid;
            spy.view = view;
            Subject.doAs(subject, spy);
            
        } catch (AccessControlException ex) {
            log.error(ex.getMessage());
            usage();
            System.exit(-1);
        } catch (IllegalArgumentException | URISyntaxException ex) {
            log.error(ex);
            usage();
        } catch (Exception unexpected) {
            log.error("FAIL", unexpected);
            System.exit(-1);
        }
    }
    
    private static void usage() {
        System.out.println("usage: spyglass [options] --resourceID=<resourceID> <direction> <target> ");
        System.out.println("                direction : pullFromVoSpace | pushToVoSpace | biDirectional");
        System.out.println("                target : an Artifact.uri");
        System.out.println("options:        [-v|--verbose|-d|--debug]");
        System.out.println("                [--cert=<proxy cert>]");
        System.out.println("                [--token=<type>:<access token> --domain=<server or domain name>]");
        System.out.println("                [--securityMethod=<uri>] : only request these security methods");
        System.out.println("                [--protocol=<uri>] : request this protocol (e.g. standardID of an API)");
        System.out.println("                [--view=<uri>] : request a specific view in the transfer");
        System.out.println("   example for sshfs mount:   --protocol=ivo://cadc.nrc.ca/vospace#SSHFS");
        System.out.println("   example for raw SI locate: --protocol=http://www.opencadc.org/std/storage#files-1.0");
        System.out.println("   example for package view:  --view=vos://cadc.nrc.ca~vospace/CADC/std/Pkg-1.0");
        
    }
    
    private static Subject getSubjectWithToken(String token, String domain) {
        String[] ss = token.split(":");
        String tokenType = ss[0];
        String tokenValue = ss[1];
        List<String> domains = new ArrayList<>();
        domains.add(domain);
        AuthorizationToken atk = new AuthorizationToken(tokenType, tokenValue, domains);
        Subject s = new Subject();
        s.getPublicCredentials().add(atk);
        return s;
    }
    
    private Main() {
    }
}
