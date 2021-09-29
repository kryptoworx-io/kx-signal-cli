package org.asamk.signal.manager.storage.prekeys;

import java.io.IOException;

public interface IPreKeyStore extends org.whispersystems.libsignal.state.PreKeyStore {

	void removeAllPreKeys();

	default void close() throws IOException { }
}