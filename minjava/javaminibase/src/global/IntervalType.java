package global;

import java.io.*;

public class intervaltype {
	
	private int s ;
	private int e ;

	public intervaltype(){
		this.s=0;
		this.e=0;
	}

	public void assign(int a, int b){
		this.s = a;
		this.e = b;
	}

	public int get_s(){
		return this.s;
	}
	public int get_e(){
		return this.e;
	}
}