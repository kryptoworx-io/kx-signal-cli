package io.kryptoworx.signalcli.api;

import org.asamk.signal.SignalCLIImpl;

public class SignalCLIFactory {
	
	public static SignalCLIFactory INSTANCE = new SignalCLIFactory();
	private SignalCLI signalCLI;
	
	private SignalCLIFactory() {
		signalCLI = new SignalCLIImpl();
	}
	
	public SignalCLI getSignalCLI() {
		return signalCLI;
	}

}
