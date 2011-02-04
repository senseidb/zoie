package org.apache.lucene.index;

import org.apache.lucene.index.DirectoryReader.MultiTermPositions;

import proj.zoie.api.ZoieIndexReader;

public class MultiZoieTermPositions extends MultiTermPositions {

	public MultiZoieTermPositions(ZoieIndexReader<?> reader, ZoieIndexReader<?>[] readers,
			int[] s) {
		super(reader, readers, s);
	}

}
