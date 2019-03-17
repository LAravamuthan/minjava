package global;

import java.util.Objects;

public class IntervalType {

    private int s;
    private int e;
    private int l;

    public IntervalType(){
        //constructor for interval type.
    }

    public void assign(int a, int b, int l) {
        try{
            if(a > 1000000 || a < -1000000 || b < -1000000 || b > 1000000){
                throw new ArithmeticException("either of the intervals a or b is not valid");
            }
            this.s = a;
            this.e = b;
            this.l = l;
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

    public int getL() {
        return l;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IntervalType that = (IntervalType) o;
        return s == that.s &&
                e == that.e &&
                l == that.l;
    }

    @Override
    public String toString() {
        return "IntervalType{" +
                "start=" + s +
                ", end=" + e +
                ", level=" + l +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(s, e, l);
    }
}
