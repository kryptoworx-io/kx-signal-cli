package org.asamk.signal.manager.storage.prekeys;

import java.io.IOException;

public interface ISignedPreKeyStore extends org.whispersystems.libsignal.state.SignedPreKeyStore {

	void removeAllSignedPreKeys();

	default void close() throws IOException { }
}