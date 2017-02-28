package question3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.RawComparator;


public class SortComparator implements RawComparator<IntWritable> {

	private static final IntWritable.Comparator TEXT_COMPARATOR = new IntWritable.Comparator();

	@Override
	public int compare(IntWritable o1, IntWritable o2) {
		return o2.compareTo(o1);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		return (-1)* TEXT_COMPARATOR.compare(b1, s1, l1, b2, s2, l2);
		
	}

}

