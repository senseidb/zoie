package proj.zoie.test;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

public class PerfTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Random rand = new Random();
		int max = 5000000;
		int[] docs = new int[max];
		for (int i=0;i<docs.length;++i)
		{
			docs[i]=i;
		}
		int limit = 10000;
		IntSet set1 = new IntRBTreeSet();
		while(set1.size() < limit)
		{
			set1.add(rand.nextInt(max));
		}
		
		IntSet set2 = new IntOpenHashSet();
		for (int i : set1)
		{
			set2.add(i);
		}
		
		int[] set3 = set1.toIntArray();
		Arrays.sort(set3);
		
		BitSet set4 = new BitSet();
		for (int i : set1)
		{
			set4.set(i);
		}
		
		long start,end;
		
		start=System.nanoTime();
		for (int i=0;i<docs.length;++i)
		{
			set1.contains(i);
		}
		end=System.nanoTime();
		System.out.println("set1: "+(end-start)/1000000);
		
		start=System.nanoTime();
		for (int i=0;i<docs.length;++i)
		{
			set2.contains(i);
		}
		end=System.nanoTime();
		System.out.println("set2: "+(end-start)/1000000);
		
		start=System.nanoTime();
		for (int i=0;i<docs.length;++i)
		{
			Arrays.binarySearch(set3, i);
		}
		end=System.nanoTime();
		System.out.println("set3: "+(end-start)/1000000);
		
		start=System.nanoTime();
		for (int i=0;i<docs.length;++i)
		{
			set4.get(i);
		}
		end=System.nanoTime();
		System.out.println("set4: "+(end-start)/1000000);
	}

}
