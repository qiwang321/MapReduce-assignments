import org.apache.hadoop.io.WritableComparator;

import edu.umd.cloud9.io.WritableComparatorUtils;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.io.pair.PairOfStrings.Comparator;

public class MyPairOfStrings extends PairOfStrings {
	
	public MyPairOfStrings() {}

	public MyPairOfStrings(String left, String right) {
		set(left, right);
	}
	
	public int compareTo(MyPairOfStrings pair) {
		String pl = pair.getLeftElement();
		String pr = pair.getRightElement();

		if (getRightElement().equals(pr)) {
			return getLeftElement().compareTo(pl);
		}

		return getRightElement().compareTo(pr);
	}
	
	public static class Comparator extends WritableComparator {

		/**
		 * Creates a new Comparator optimized for <code>MyPairOfStrings</code>.
		 */
		public Comparator() {
			super(MyPairOfStrings.class);
		}

		/**
		 * Optimization hook.
		 */
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			
			int s1offset = readUnsignedShort(b1, s1);
			int s2offset = readUnsignedShort(b2, s2);
			
			String thisRightValue = WritableComparatorUtils.readUTF(b1, s1 + 2 + s1offset);
			String thatRightValue = WritableComparatorUtils.readUTF(b2, s2 + 2 + s2offset);
			
			if (thisRightValue.equals(thatRightValue)) {
				String thisLeftValue = WritableComparatorUtils.readUTF(b1, s1);
				String thatLeftValue = WritableComparatorUtils.readUTF(b2, s2);	
				return thisLeftValue.compareTo(thatLeftValue);
			}

			return thisRightValue.compareTo(thatRightValue);
		}
	}

	static { // register this comparator
		WritableComparator.define(MyPairOfStrings.class, new Comparator());
	}
}