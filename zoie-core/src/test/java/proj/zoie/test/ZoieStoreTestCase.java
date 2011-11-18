package proj.zoie.test;

import java.nio.charset.Charset;

import junit.framework.Assert;

import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import proj.zoie.store.AbstractZoieStore;
import proj.zoie.store.LuceneStore;
import proj.zoie.store.ZoieStore;

public class ZoieStoreTestCase {

	static final Charset UTF8 = Charset.forName("UTF-8");
	@Test
	public void testCompression() throws Exception{
		String s1 = "some compression data";
		byte[] compressed = AbstractZoieStore.compress(s1.getBytes(UTF8));
		byte[] uncompressed = AbstractZoieStore.uncompress(compressed);
		String s2 = new String(uncompressed,UTF8);
		Assert.assertEquals(s1, s2);
	}
	
	@Test
	public void testSanity() throws Exception{
		String s1 = "somedata";
		RAMDirectory ramDir = new RAMDirectory();
		boolean compressionOff = false;
		ZoieStore store = LuceneStore.openStore(ramDir, "test", compressionOff);
		store.put(1, s1.getBytes(UTF8), "1");
		Assert.assertEquals("1", store.getVersion());
		Assert.assertNull(store.getPersistedVersion());
		byte[] data = store.get(1);
		Assert.assertNotNull(data);
		Assert.assertEquals(s1, new String(data,UTF8));
		store.commit();
		Assert.assertEquals("1", store.getPersistedVersion());
		data = store.get(1);
		Assert.assertNotNull(data);
		Assert.assertEquals(s1, new String(data,UTF8));
		
		store.delete(1, "2");
		Assert.assertEquals("2", store.getVersion());
		Assert.assertEquals("1", store.getPersistedVersion());
		data = store.get(1);
		Assert.assertNull(data);
		store.commit();
		Assert.assertEquals("2", store.getPersistedVersion());
		data = store.get(1);
		Assert.assertNull(data);
		store.close();
	}
}
