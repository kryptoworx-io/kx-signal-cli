package org.asamk.signal.manager.storage.prekeys;


public interface IPreKeyStore extends org.whispersystems.libsignal.state.PreKeyStore {

    void removeAllPreKeys();
    int getPreKeyIdOffset();

}