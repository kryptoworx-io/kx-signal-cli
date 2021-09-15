package org.asamk.signal.manager.storage.prekeys;

import java.util.List;

import org.whispersystems.libsignal.InvalidKeyIdException;
import org.whispersystems.libsignal.state.SignedPreKeyRecord;

public interface ISignedPreKeyStore {
    SignedPreKeyRecord loadSignedPreKey(int signedPreKeyId) throws InvalidKeyIdException;

    List<SignedPreKeyRecord> loadSignedPreKeys();

    void storeSignedPreKey(int signedPreKeyId, SignedPreKeyRecord record);

    boolean containsSignedPreKey(int signedPreKeyId);

    void removeSignedPreKey(int signedPreKeyId);

    void removeAllSignedPreKeys();

}