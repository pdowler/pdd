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

import ca.nrc.cadc.ac.ACIdentityManager;
import ca.nrc.cadc.auth.AuthenticationUtil;
import ca.nrc.cadc.auth.IdentityManager;
import ca.nrc.cadc.auth.SSLUtil;
import ca.nrc.cadc.db.ConnectionConfig;
import ca.nrc.cadc.db.DBConfig;
import ca.nrc.cadc.db.DBUtil;
import ca.nrc.cadc.util.ArgumentMap;
import ca.nrc.cadc.util.Log4jInit;
import ca.nrc.cadc.vos.server.db.DatabaseNodePersistence;
import ca.nrc.cadc.vospace.VOSpaceNodePersistence;
import java.io.File;
import java.net.URI;
import java.security.PrivilegedActionException;
import java.util.List;
import java.util.UUID;
import javax.security.auth.Subject;
import javax.sql.DataSource;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.opencadc.inventory.db.version.InitDatabaseSI;
import org.opencadc.vault.NodePersistenceImpl;
import org.opencadc.vospace.db.InitDatabaseVOS;

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
                Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.INFO);
                Log4jInit.setLevel("org.opencadc.vault.migrate", Level.INFO);
                
            } else if (am.isSet("d") || am.isSet("debug")) {
                Log4jInit.setLevel("ca.nrc.cadc.db.version", Level.DEBUG);
                Log4jInit.setLevel("org.opencadc.vault.migrate", Level.DEBUG);
            } 

            boolean help = am.isSet("h") || am.isSet("help");
            
            if (help) {
                usage();
                System.exit(0);
            }
            
            final boolean recursive = am.isSet("recursive");
            final List<String> nodes = am.getPositionalArgs();
            if (recursive && nodes.isEmpty()) {
                System.out.println("INVALID: cannot use recursive mode without specifying 1 or more top level containers");
                usage();
                System.exit(-1);
            }
            
            // local config
            File altHomeDir = new File("servops");
            if (altHomeDir.exists() && altHomeDir.isDirectory()) {
                System.setProperty("user.home", altHomeDir.getAbsolutePath());
            } else {
                log.error("not found: servops (alt home dir)");
                System.exit(-1);
            }
            log.info("alt home dir: " + System.getProperty("user.home"));
            
            //System.setProperty(IdentityManager.class.getName(), ACIdentityManager.class.getName());
            //File cert = new File(System.getProperty("user.home") + "/.ssl/cadcproxy.pem");
            //Subject subject = SSLUtil.createSubject(cert);
            Subject subject = AuthenticationUtil.getAnonSubject();
            log.info("running as: " + subject);
            
            String str = am.getValue("threads");
            int threads = 1;
            if (str != null) {
                threads = Integer.parseInt(str);
            }
            
            // need one source and one destination connection per thread
            DBConfig dbrc = new DBConfig();
            ConnectionConfig syb = dbrc.getConnectionConfig("SYBVAULT", "vospace2");
            DBUtil.PoolConfig sybpool = new DBUtil.PoolConfig(syb, threads, 20000L, "select 123");
            DBUtil.createJNDIDataSource(VOSpaceNodePersistence.DATASOURCE_NAME, sybpool);
            final DatabaseNodePersistence src = new VOSpaceNodePersistence();
            log.info("source ready: " + syb.getServer() + " " + syb.getDatabase() + "\n");
            
            ConnectionConfig pg = dbrc.getConnectionConfig("PGVAULT", am.getValue("pgdb"));
            DBUtil.PoolConfig pgpool = new DBUtil.PoolConfig(pg, threads, 20000L, "select 123");
            DBUtil.createJNDIDataSource("jdbc/nodes", pgpool);
            DataSource vds = DBUtil.findJNDIDataSource("jdbc/nodes");
            
            log.info("init database for vospace: START");
            InitDatabaseVOS vs = new InitDatabaseVOS(vds, null, "vospace");
            vs.doInit();
            log.info("init database for vospace: OK");
            
            final NodePersistenceImpl dest = new NodePersistenceImpl(URI.create("ivo://cadc.nrc.ca/vault"), "vault-migrate");
            dest.nodeOrigin = false; // preserve lastModified timestamps
            log.info("destination ready: " + pg.getServer() + " " + pg.getDatabase() + "\n");
            
            Migrate mig = new Migrate(src, dest);
            mig.setRecursive(recursive);
            mig.setDeletions(am.isSet("deletions"));
            mig.setDryrun(am.isSet("dryrun"));
            mig.setNodes(nodes);
            mig.setThreads(threads);
            try {
                Subject.doAs(subject, mig);
            } catch (PrivilegedActionException pex) {
                throw pex.getException();
            }
            
        } catch (Exception unexpected) {
            log.error("FAIL", unexpected);
            usage();
            System.exit(-1);
        }
    }
    
    private static void usage() {
        System.out.println("usage: vault-migrate --pgdb=<destination database name> [options] [--dryrun] ...");
        System.out.println("        --recursive <container node> [<container node> ...] : migrate specified nodes");
        System.out.println("        --deletions : process DeletedNodeEvent(s) from source");
        System.out.println("options:");
        System.out.println("        [-v|--verbose|-d|--debug]");
        System.out.println("        [--threads=<int>] : number of migrate threads (default: 1)");
    }
}
