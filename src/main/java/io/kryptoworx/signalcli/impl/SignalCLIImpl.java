package io.kryptoworx.signalcli.impl;

import io.kryptoworx.signalcli.api.SignalCLI;

public class SignalCLIImpl implements SignalCLI {
	
	
    @Override
    public void register(final String number) {
    	System.out.println("in register for number: " + number);
    }

    @Override
    public void send(final String message, final String targetNumber) {
    	System.out.println("in sending message: ");
    } 

	
}
