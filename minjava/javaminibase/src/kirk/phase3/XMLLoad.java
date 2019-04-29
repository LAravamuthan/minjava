package phase3;

import global.*;
import bufmgr.*;


import java.lang.*;

public class XMLLoad implements GlobalConst {

    public static void main(String args[]) {
        String dataFileName;
        String xmlFilename;

        if (args.length == 0) {
            System.out.println("Usage:\n\n  java -classpath .:..:../* phase3.XMLLoad " +
                    "[full path to database file] [path to XML file to import]\n\n");
            System.out.println("  $ java -classpath .:..:../* phase3.XMLLoad test." +
                    "db xml_sample_data.xml");
        } else {
            dataFileName = args[0];
            xmlFilename = args[1];

            try {
                XMLDriver xmldvr = new XMLDriver(dataFileName,xmlFilename);

                NodeContext MainTagPair = xmldvr.ReadFileLbyLStoreInHeapFile();
                
                System.out.println("Reads: " + PageCounter.getreads());
                System.out.println("Writes: " + PageCounter.getwrites());
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error encountered during XML DB Import:\n");
                Runtime.getRuntime().exit(1);
            }
        }
	Runtime.getRuntime().exit(0);
    }
}
