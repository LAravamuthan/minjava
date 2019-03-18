package global;

import java.util.Objects;

public class IntervalType {

    private int s;
    private int e;

    public IntervalType(){
        //constructor for interval type.
    }



    public void assign(int a, int b) {
        try{
            if(a > 1000000 || a < -1000000 || b < -1000000 || b > 1000000){
                throw new ArithmeticException("either of the intervals a or b is not valid");
            }
            this.s = a;
            this.e = b;
        }catch (ArithmeticException e){
            System.out.println(e);
        }
    }

    public int getS() {
        return s;
    }

    public int getE() {
        return e;
    }

    public void setS(int s) {
        this.s = s;
    }

    public void setE(int e) {
        this.e = e;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntervalType that = (IntervalType) o;
        return s == that.s &&
                e == that.e;
    }

    @Override
    public int hashCode() {
        return Objects.hash(s, e);
    }

    @Override
    public String toString() {
        return "IntervalType{" +
                "s=" + s +
                ", e=" + e +
                '}';
    }
}
