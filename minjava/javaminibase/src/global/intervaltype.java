package global;

import java.io.*;

public class intervaltype {
	
	private int s ;
	private int e ;

	public intervaltype(){
		this.s=0;
		this.e=0;
	}

	public intervaltype(int st, int ed){
		this.s=st;
		this.e=ed;
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

	public intervaltype(intervaltype obj)
	{
		this.s = obj.get_s();
		this.e = obj.get_e();
	}

}