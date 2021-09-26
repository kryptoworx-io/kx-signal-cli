package io.kryptoworx.signalcli.api;

import org.asamk.signal.SignalCLIImpl;

public class SignalCLIFactory {

	public static SignalCLIFactory INSTANCE = new SignalCLIFactory();
	private SignalCLI signalCLI;

	private SignalCLIFactory() {
		try {
			signalCLI = new SignalCLIImpl();
		} catch (Exception ex) {
			// Swallow here, we throw in the getter.
		}
	}

	public SignalCLI getSignalCLI() {
		if (signalCLI == null) {
			throw new IllegalStateException("The SignalCLI implementation is not available!");
		}
		return signalCLI;
	}

}
