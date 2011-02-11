package org.apache.lucene.index;

import org.apache.lucene.index.DirectoryReader.MultiTermDocs;

import proj.zoie.api.ZoieIndexReader;

public class MultiZoieTermDocs extends MultiTermDocs {

	public MultiZoieTermDocs(ZoieIndexReader<?> reader, ZoieIndexReader<?>[] readers, int[] s) {
		super(reader, readers, s);
	}

}
