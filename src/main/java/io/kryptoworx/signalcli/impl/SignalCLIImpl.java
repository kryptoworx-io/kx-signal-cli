package io.kryptoworx.signalcli.impl;

import java.io.IOException;
import java.security.Security;

import org.asamk.signal.manager.RegistrationManager;
import org.asamk.signal.util.SecurityProvider;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import io.kryptoworx.signalcli.api.SignalCLI;

public class SignalCLIImpl implements SignalCLI {
	
	RegistrationManager registrationManager = null;
	
	public SignalCLIImpl() {
		try {
			Security.setProperty("crypto.policy", "unlimited");
			Security.insertProviderAt(new SecurityProvider(), 1);
		    Security.addProvider(new BouncyCastleProvider());
			registrationManager = RegistrationManager.initInternal("+4368120784581");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
    @Override
    public void register(final String number) {
    	System.out.println("in register for number: " + number);
    	// TODO remove when FCM is integrated.
    	String captcha = System.getProperty("captcha");
    	try {
			registrationManager.register(false, captcha);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public void send(final String message, final String targetNumber) {
    	System.out.println("in sending message: ");
    } 

	
}
