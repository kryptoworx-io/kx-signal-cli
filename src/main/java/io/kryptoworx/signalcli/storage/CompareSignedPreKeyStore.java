package io.kryptoworx.signalcli.storage;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.asamk.signal.manager.storage.prekeys.ISignedPreKeyStore;
import org.whispersystems.libsignal.InvalidKeyIdException;
import org.whispersystems.libsignal.state.SignedPreKeyRecord;

public class CompareSignedPreKeyStore implements ISignedPreKeyStore {
    private final ISignedPreKeyStore origStore;
    private final ISignedPreKeyStore newStore;
    
    public CompareSignedPreKeyStore(ISignedPreKeyStore origStore, ISignedPreKeyStore newStore) {
        this.origStore = origStore;
        this.newStore = newStore;
    }

    public SignedPreKeyRecord loadSignedPreKey(int signedPreKeyId) throws InvalidKeyIdException {
        var r1 = origStore.loadSignedPreKey(signedPreKeyId);
        var r2 = newStore.loadSignedPreKey(signedPreKeyId);
        assert(equals(r1, r2));
        return r2;
    }

    public List<SignedPreKeyRecord> loadSignedPreKeys() {
        var r1 = origStore.loadSignedPreKeys();
        var r2 = newStore.loadSignedPreKeys();
        assert (CompareSessionStore.equals(r1, r2, CompareSignedPreKeyStore::equals));
        return r2;
    }

    public void storeSignedPreKey(int signedPreKeyId, SignedPreKeyRecord record) {
        origStore.storeSignedPreKey(signedPreKeyId, record);
        newStore.storeSignedPreKey(signedPreKeyId, record);
    }

    public boolean containsSignedPreKey(int signedPreKeyId) {
        var r1 = origStore.containsSignedPreKey(signedPreKeyId);
        var r2 = newStore.containsSignedPreKey(signedPreKeyId);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public void removeSignedPreKey(int signedPreKeyId) {
        origStore.removeSignedPreKey(signedPreKeyId);
        newStore.removeSignedPreKey(signedPreKeyId);
    }

    public void removeAllSignedPreKeys() {
        origStore.removeAllSignedPreKeys();
        newStore.removeAllSignedPreKeys();
    }
    
    private static boolean equals(SignedPreKeyRecord r1, SignedPreKeyRecord r2) {
        if (r1 == null) return r2 == null;
        if (r2 == null) return false;
        return Arrays.equals(r1.serialize(), r2.serialize());
    }
}
