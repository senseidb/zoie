package proj.zoie.perf.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermEnum;
import org.apache.lucene.store.FSDirectory;

import proj.zoie.api.impl.util.PriorityQueue;

public class TermFileBuilder {

	private static class TermWithFreq{
		final String term;
		final int count;
		
		TermWithFreq(String term,int count){
			this.term = term;
			this.count = count;
		}
	}
	
	public static List<String> loadFile(File infile) throws Exception{
		LinkedList<String> terms = new LinkedList<String>();
		if (infile.exists()){
		  BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(infile),Charset.forName("UTF-8")));
		
		  while(true){
			String line = reader.readLine();
			if (line==null) break;
			terms.add(line);
		  }
		}
		return terms;
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		File idxDir = new File(args[0]);
		File outFile = new File("terms.txt");
		
		String field = "contents";
		
		int capacity = 1000;
		
		PriorityQueue<TermWithFreq> pq = new PriorityQueue<TermWithFreq>(capacity, new Comparator<TermWithFreq>(){

			@Override
			public int compare(TermWithFreq o1, TermWithFreq o2) {
				if (o1.count == o2.count){
					return o1.term.compareTo(o2.term);
				}
				return o2.count - o1.count;
			}
			
		});
		
		IndexReader reader = IndexReader.open(FSDirectory.open(idxDir), true);
		TermEnum te = reader.terms(new Term(field,""));
		while(te.next()){
			Term term = te.term();
			if (!term.field().equals(field)){
				break;
			}
			String text = term.text();
			if (text!=null && text.length()>0){
				int freq = te.docFreq();
				if (freq>0){
					pq.offer(new TermWithFreq(text,freq));
				}
			}
		}
		
		te.close();
		reader.close();
		
		OutputStreamWriter owriter = new OutputStreamWriter(new FileOutputStream(outFile),Charset.forName("UTF-8"));
		PrintWriter pwriter = new PrintWriter(owriter);
		
		Iterator<TermWithFreq> iter = pq.iterator();
		
		while(iter.hasNext()){
			TermWithFreq t = iter.next();
			pwriter.println(t.term);
		}
		
		pwriter.flush();
		pwriter.close();
	}
}
