package org.asamk.signal.manager.storage.prekeys;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.libsignal.InvalidKeyIdException;
import org.whispersystems.libsignal.state.PreKeyRecord;

import io.kryptoworx.signalcli.storage.H2Map;

public class PreKeyStore extends H2Map<Integer, PreKeyRecord> implements org.whispersystems.libsignal.state.PreKeyStore, AutoCloseable {

    private final static Logger logger = LoggerFactory.getLogger(PreKeyStore.class);
    
    public PreKeyStore(DataSource dataSource) {
    	super(dataSource, "prekeys", 
    			(id, key) -> key.serialize(), 
    			(id, bs)  -> deserialize(bs));
    }
    
	@Override
	protected Column<Integer> createPrimaryKeyColumn() {
		return new Column<>("id", "INT", PreparedStatement::setInt, ResultSet::getInt);
	}

    @Override
    public PreKeyRecord loadPreKey(int preKeyId) throws InvalidKeyIdException {
    	PreKeyRecord record = get(preKeyId);
        if (record == null) {
            throw new InvalidKeyIdException("No such pre-key record!");
        }
        return record;
    }

    @Override
    public void storePreKey(int preKeyId, PreKeyRecord record) {
    	put(preKeyId, record);
    }

    @Override
    public boolean containsPreKey(int preKeyId) {
    	return containsKey(preKeyId);
    }

    @Override
    public void removePreKey(int preKeyId) {
    	remove(preKeyId);
    }

    public void removeAllPreKeys() {
    	remove(null);
    }

	private static PreKeyRecord deserialize(byte[] bytes) {
		try {
			return new PreKeyRecord(bytes);
		} catch (IOException e) {
			String msg = "Failed to decode pre-key";
			logger.error(msg, e);
            throw new AssertionError(msg, e);
		}
	}
}
