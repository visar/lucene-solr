package org.apache.solr.cloud;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import org.apache.solr.common.cloud.OnReconnect;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class BBZKUploader
{
    private static final String ZK_CLI_NAME = "ZkCLI";
    private static final String CONFDIR     = "confdir";
    private static final String CONFNAME    = "confname";
    private static final String ZKHOST      = "zkhost";
    private static final String UPCONFIG    = "upconfig";
    private static final String CMD         = "cmd";

    private static void uploadToZK(SolrZkClient zkClient, File dir, String zkPath)
            throws IOException, KeeperException, InterruptedException
    {
        File[] files = dir.listFiles();
        if (files == null)
        {
            throw new IllegalArgumentException("Illegal directory: " + dir);
        }
        for (File file : files)
        {
            if (!file.getName().startsWith(".") && !file.getName().endsWith(".sav"))
            {
                if (!file.isDirectory())
                {
                    zkClient.makePath(zkPath + "/" + file.getName(), file,
                            false, true);
                }
                else
                {
                    uploadToZK(zkClient, file, zkPath + "/" + file.getName());
                }
            }
        }
    }

    /**
     * Allows you to perform a variety of zookeeper related tasks, such as:
     *
     * Bootstrap the current configs for all collections in solr.xml.
     *
     * Upload a named config set from a given directory.
     *
     * Link a named config set explicity to a collection.
     *
     * Clear ZooKeeper info.
     *
     * If you also pass a solrPort, it will be used to start an embedded zk
     * useful for single machine, multi node tests.
     */
    public static void main(String[] args) throws InterruptedException,
            TimeoutException, IOException, ParserConfigurationException,
            SAXException, KeeperException
    {

        CommandLineParser parser = new PosixParser();
        Options options = new Options();

        options.addOption(OptionBuilder.hasArg(true).withDescription("cmd to run: " + UPCONFIG).create(CMD));

        Option zkHostOption = new Option("z", ZKHOST, true,
                "ZooKeeper host address");
        options.addOption(zkHostOption);

        options.addOption("d", CONFDIR, true, "for " + UPCONFIG + ": a directory of configuration files");
        options.addOption("n", CONFNAME, true, "for " + UPCONFIG + ": name of the config set");

        try
        {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            if (!line.hasOption(ZKHOST))
            {
                // automatically generate the help statement
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(ZK_CLI_NAME, options);
                System.out.println("Examples:");
                System.out.println("zkcli.sh -zkhost localhost:9983 -" + CONFDIR + " /opt/solr/collection1/conf" + " -" + CONFNAME + " myconf");
                return;
            }

            String zkServerAddress = line.getOptionValue(ZKHOST);

            SolrZkClient zkClient = null;
            try
            {
                zkClient = new SolrZkClient(zkServerAddress, 15000 /*zkClientTimeout*/, 10000 /*zkClientConnectTimeout*/,
                        new OnReconnect()
                            {
                                @Override
                                public void command()
                                {
                                }
                            });

                if (!line.hasOption(CONFDIR) || !line.hasOption(CONFNAME))
                {
                    System.out.println("-" + CONFDIR + " and -" + CONFNAME
                            + " are required for " + UPCONFIG);
                    System.exit(1);
                }
                String confDir = line.getOptionValue(CONFDIR);
                String confName = line.getOptionValue(CONFNAME);

                // Check if a chroot path exists in ZK, and create it if necessary.
                // http://zookeeper.apache.org/doc/r3.2.2/zookeeperProgrammers.html#ch_zkSessions
                // for more details of chroot'ed paths within ZK.
                if (!ZkController.checkChrootPath(zkServerAddress, true))
                {
                    System.out.println("A chroot was specified in zkHost but the znode doesn't exist. ");
                    System.exit(1);
                }

                File directory = new File(confDir);
                String zkPath = ZkController.CONFIGS_ZKNODE + "/" + confName;

                uploadToZK(zkClient, directory, zkPath);
            }
            finally
            {
                if (zkClient != null)
                {
                    zkClient.close();
                }
            }
        }
        catch (ParseException exp)
        {
            System.out.println("Unexpected exception:" + exp.getMessage());
        }
    }
}
