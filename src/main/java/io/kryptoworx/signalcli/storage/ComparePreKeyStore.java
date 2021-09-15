package io.kryptoworx.signalcli.storage;

import java.util.Arrays;
import java.util.Objects;

import org.asamk.signal.manager.storage.prekeys.IPreKeyStore;
import org.whispersystems.libsignal.InvalidKeyIdException;
import org.whispersystems.libsignal.state.PreKeyRecord;

public class ComparePreKeyStore implements IPreKeyStore {

    private final IPreKeyStore origStore;
    private final IPreKeyStore newStore;
    
    public ComparePreKeyStore(IPreKeyStore origStore, IPreKeyStore newStore) {
        this.origStore = origStore;
        this.newStore = newStore;
    }

    public PreKeyRecord loadPreKey(int preKeyId) throws InvalidKeyIdException {
        var r1 = origStore.loadPreKey(preKeyId);;
        var r2 = newStore.loadPreKey(preKeyId);;
        assert(equals(r1, r2));
        return r2;
    }

    public void storePreKey(int preKeyId, PreKeyRecord record) {
        origStore.storePreKey(preKeyId, record);
        newStore.storePreKey(preKeyId, record);
    }

    public boolean containsPreKey(int preKeyId) {
        var r1 = origStore.containsPreKey(preKeyId);;
        var r2 = newStore.containsPreKey(preKeyId);;
        assert(Objects.equals(r1, r2));
        return r2;
    }

    public void removePreKey(int preKeyId) {
        origStore.removePreKey(preKeyId);
        newStore.removePreKey(preKeyId);
    }

    public void removeAllPreKeys() {
        origStore.removeAllPreKeys();
        newStore.removeAllPreKeys();
    }

    private static boolean equals(PreKeyRecord r1, PreKeyRecord r2) {
        if (r1 == null) return r2 == null;
        if (r2 == null) return false;
        return Arrays.equals(r1.serialize(), r2.serialize());
    }

}
