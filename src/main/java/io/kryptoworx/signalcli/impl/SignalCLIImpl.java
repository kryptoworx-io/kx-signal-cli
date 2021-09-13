package io.kryptoworx.signalcli.impl;

import java.io.IOException;
import java.security.Security;

import org.asamk.signal.manager.RegistrationManager;
import org.asamk.signal.util.SecurityProvider;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.whispersystems.signalservice.api.KeyBackupServicePinException;
import org.whispersystems.signalservice.api.KeyBackupSystemNoDataException;
import org.whispersystems.signalservice.internal.push.LockedException;

import io.kryptoworx.signalcli.api.SignalCLI;

public class SignalCLIImpl implements SignalCLI {
	
	RegistrationManager registrationManager = null;
	
	public SignalCLIImpl() {
		try {
			Security.setProperty("crypto.policy", "unlimited");
			Security.insertProviderAt(new SecurityProvider(), 1);
		    Security.addProvider(new BouncyCastleProvider());
			registrationManager = RegistrationManager.initInternal("+4368120784581");
			
			var m = loadManager("+4368120784581", dataPath, serviceEnvironment, trustNewIdentity
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
    @Override
    public void register(final String phoneNumber) {
    	System.out.println("in register for number: " + phoneNumber);
    	try {
			registrationManager.register(false, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    @Override
    public void send(final String message, final String targetNumber) {
    	System.out.println("in sending message: ");
    	
    }


	@Override
	public void verify(String code) {
		try {
			registrationManager.verifyAccount(code, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
			
		
	}


	@Override
	public void registerWithCaptcha(String phoneNumber, String captcha) {
		// TODO remove when FCM is integrated.
    	try {
			registrationManager.register(false, captcha);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 

	
}
