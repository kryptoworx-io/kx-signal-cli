package org.asamk.signal.manager.storage.recipients;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.asamk.signal.manager.storage.prekeys.PreKeyStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.whispersystems.libsignal.ecc.Curve;
import org.whispersystems.libsignal.state.PreKeyRecord;

public class PreKeyStoreTest extends H2Test {
	
	private static PreKeyStore store;
	private static TestDataSource dataSource;
	
	@BeforeClass
	public static void setup() throws IOException {
		dataSource = createDataSource();
		store = new PreKeyStore(dataSource);
	}
	
	@Test
	public void test1() throws Exception {
		var keyPair = Curve.generateKeyPair();
        var key = new PreKeyRecord(0, keyPair);
		store.storePreKey(0, key);
		PreKeyRecord keyFromStore = store.loadPreKey(0);
		assertTrue(Arrays.equals(key.serialize(), keyFromStore.serialize()));
	}
	
	@AfterClass
	public static void tearDown() throws IOException {
		dataSource.close();
	}
}
