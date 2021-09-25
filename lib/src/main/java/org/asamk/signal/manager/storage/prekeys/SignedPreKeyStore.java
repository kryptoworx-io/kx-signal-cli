package org.asamk.signal.manager.storage.prekeys;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.libsignal.InvalidKeyIdException;
import org.whispersystems.libsignal.state.SignedPreKeyRecord;

import io.kryptoworx.signalcli.storage.H2Map;

public class SignedPreKeyStore extends H2Map<Integer, SignedPreKeyRecord> implements org.whispersystems.libsignal.state.SignedPreKeyStore, AutoCloseable {

    private final static Logger logger = LoggerFactory.getLogger(SignedPreKeyStore.class);

    public SignedPreKeyStore(DataSource dataSource) {
    	super(dataSource, "signed_prekeys", 
    			(id, key) -> key.serialize(), 
    			(id, bs)  -> deserialize(bs));
   }

	@Override
	protected Column<Integer> createPrimaryKeyColumn() {
		return new Column<>("id", "INT", PreparedStatement::setInt, ResultSet::getInt);
	}

	@Override
    public SignedPreKeyRecord loadSignedPreKey(int signedPreKeyId) throws InvalidKeyIdException {
		SignedPreKeyRecord record = get(signedPreKeyId);
        if (record == null) {
            throw new InvalidKeyIdException("No such signed pre-key record!");
        }
        return record;
    }


    @Override
    public List<SignedPreKeyRecord> loadSignedPreKeys() {
    	return getAll();
    }

    @Override
    public void storeSignedPreKey(int signedPreKeyId, SignedPreKeyRecord record) {
        put(signedPreKeyId, record);
    }

    @Override
    public boolean containsSignedPreKey(int signedPreKeyId) {
    	return containsKey(signedPreKeyId);
    }

    @Override
    public void removeSignedPreKey(int signedPreKeyId) {
    	remove(signedPreKeyId);
    }

    public void removeAllSignedPreKeys() {
    	remove(null);
    }

	private static SignedPreKeyRecord deserialize(byte[] bytes) {
		try {
			return new SignedPreKeyRecord(bytes);
		} catch (IOException e) {
			String msg = "Failed to decode signed pre-key";
			logger.error(msg, e);
            throw new AssertionError(msg, e);
		}
	}

}
