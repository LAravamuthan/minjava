package bufmgr;

public class PageCounter {
	
	public static int readCounter;
	public static int writeCounter;

	public static void initialize() {
		readCounter =0;
		writeCounter =0;
	}

	public static void readIncrement() {
		System.out.println("readCounter : "+ readCounter);
		readCounter++;
	}

	public static int getreads(){
		return readCounter;
	}

	public static void writeIncrement() {
		System.out.println("writeCounter : "+ writeCounter);
		writeCounter++;
	}

	public static int getwrites(){
		return writeCounter;
	}

}