package question3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextPair implements WritableComparable<TextPair>{  
    public  Text first;
	public  Text second;
	
	public  TextPair(){
	    this.first=new Text();
	    this.second=new Text();
	}
	
	public TextPair(Text first, Text second) {
	    //super();
	    this.first = first;
	    this.second = second;
	}
	public TextPair(String first,String second){
	    this.first=new Text(first);
	    this.second=new Text(second);
	}
	
	public Text getFirst() {
	    return first;
	}
	
	public void setFirst(Text first) {
	    this.first = first;
	}
	
	public Text getSecond() {
	    return second;
	}
	
	public void setSecond(Text second) {
	    this.second = second;
	}
	public void set(Text first,Text second){
	    this.first=first;
	    this.second=second;
	}
	
	@Override
	public int hashCode() {
	    // TODO Auto-generated method stub
	    return first.hashCode()*163+second.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
	    // TODO Auto-generated method stub
	    if(obj instanceof TextPair){
	        TextPair tp=(TextPair)obj;
	        return first.equals(tp.getFirst())&&second.equals(tp.getSecond());
	    }
	    return false;
	}
	
	@Override
	public String toString() {
	    // TODO Auto-generated method stub
	    return first+"\t"+second;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
	    // TODO Auto-generated method stub
	    first.readFields(in);
	    second.readFields(in);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
	    // TODO Auto-generated method stub
	    first.write(out);
	    second.write(out);
	}
	
	
	
	
	@Override
	public int compareTo(TextPair tp) {
	    // TODO Auto-generated method stub
	    int cmp=first.compareTo(tp.getFirst());
	    if(cmp!=0)
	        return cmp;
	    return second.compareTo(tp.getSecond());
	    
	}
	
	public TextPair reverse() {
		return new TextPair(second,first);
	}
	
}
