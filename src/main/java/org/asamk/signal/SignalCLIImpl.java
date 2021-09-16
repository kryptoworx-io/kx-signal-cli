package org.asamk.signal;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.asamk.signal.commands.exceptions.CommandException;
import org.asamk.signal.commands.exceptions.IOErrorException;
import org.asamk.signal.commands.exceptions.UnexpectedErrorException;
import org.asamk.signal.commands.exceptions.UserErrorException;
import org.asamk.signal.manager.AttachmentInvalidException;
import org.asamk.signal.manager.Manager;
import org.asamk.signal.manager.NotRegisteredException;
import org.asamk.signal.manager.PathConfig;
import org.asamk.signal.manager.RegistrationManager;
import org.asamk.signal.manager.api.Message;
import org.asamk.signal.manager.api.RecipientIdentifier;
import org.asamk.signal.manager.config.ServiceConfig;
import org.asamk.signal.manager.config.ServiceEnvironment;
import org.asamk.signal.manager.groups.GroupNotFoundException;
import org.asamk.signal.manager.groups.GroupSendingNotAllowedException;
import org.asamk.signal.manager.groups.NotAGroupMemberException;
import org.asamk.signal.manager.storage.identities.TrustNewIdentity;
import org.asamk.signal.util.CommandUtil;
import org.asamk.signal.util.ErrorUtils;
import org.asamk.signal.util.IOUtils;
import org.asamk.signal.util.SecurityProvider;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.whispersystems.signalservice.api.KeyBackupServicePinException;
import org.whispersystems.signalservice.api.KeyBackupSystemNoDataException;
import org.whispersystems.signalservice.internal.push.LockedException;

import io.kryptoworx.signalcli.api.SignalCLI;

public class SignalCLIImpl implements SignalCLI {

	RegistrationManager registrationManager = null;
	Manager generalManager = null;
	String userName = null;

	public SignalCLIImpl() {
		try {
			Security.setProperty("crypto.policy", "unlimited");
			Security.insertProviderAt(new SecurityProvider(), 1);
			Security.addProvider(new BouncyCastleProvider());

			var usernames = Manager.getAllLocalUsernames(getDefaultDataPath());
			userName = usernames.get(0);

			
			

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private File getDefaultDataPath() {
		return new File(IOUtils.getDataHomeDir(), "signal-cli");
	}

	private Manager loadManager(final String username, final File dataPath, final ServiceEnvironment serviceEnvironment,
			final TrustNewIdentity trustNewIdentity) throws CommandException {
		Manager manager;
		try {
			manager = Manager.init(username, dataPath, serviceEnvironment, BaseConfig.USER_AGENT, trustNewIdentity);
		} catch (NotRegisteredException e) {
			throw new UserErrorException("User " + username + " is not registered.");
		} catch (Throwable e) {
			throw new UnexpectedErrorException("Error loading state file for user " + username + ": " + e.getMessage()
					+ " (" + e.getClass().getSimpleName() + ")", e);
		}

		try {
			manager.checkAccountState();
		} catch (IOException e) {
			throw new IOErrorException("Error while checking account " + username + ": " + e.getMessage(), e);
		}

		return manager;
	}

	@Override
	public void register(final String phoneNumber) {
		System.out.println("in register for number: " + phoneNumber);
		
		try {
			registrationManager = RegistrationManager.initInternal(userName);
			registrationManager.register(false, null);
			registrationManager.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void send(final String message, final String targetNumber, final String attachmentFileName) {
		
		try {
		System.out.println("in sending message: ");
		
		generalManager = loadManager(userName, getDefaultDataPath(), ServiceEnvironment.LIVE,
				TrustNewIdentity.ON_FIRST_USE);
		var m = generalManager;

		final var recipientStrings = Collections.singletonList(targetNumber);

		final Set<RecipientIdentifier> recipientIdentifiers = CommandUtil.getRecipientIdentifiers(m, false, recipientStrings,
				Collections.EMPTY_LIST);

		final var isEndSession = false;
		if (isEndSession) {
			final Set singleRecipients = recipientIdentifiers.stream()
					.filter(r -> r instanceof RecipientIdentifier.Single).map(RecipientIdentifier.Single.class::cast)
					.collect(Collectors.toSet());
			if (( singleRecipients).isEmpty()) {
				throw new UserErrorException("No recipients given");
			}

			try {
				m.sendEndSessionMessage(singleRecipients);
				m.close();
				return;
			} catch (IOException e) {
				throw new UnexpectedErrorException(
						"Failed to send message: " + e.getMessage() + " (" + e.getClass().getSimpleName() + ")", e);
			}
		}

		var messageText = message;
		if (messageText == null) {
			try {
				messageText = IOUtils.readAll(System.in, Charset.defaultCharset());
			} catch (IOException e) {
				throw new UserErrorException("Failed to read message from stdin: " + e.getMessage());
			}
		}

		List<String> attachments = attachmentFileName == null ? Collections.emptyList() : Collections.singletonList(attachmentFileName);
		if (attachments == null) {
			attachments = List.of();
		}

		var outputWriter = new PlainTextWriterImpl(System.out);
		
		try {
			var results = m.sendMessage(new Message(messageText, attachments), recipientIdentifiers);
			outputResult(outputWriter, results.getTimestamp());
			m.close();
			ErrorUtils.handleSendMessageResults(results.getResults());
		} catch (AttachmentInvalidException | IOException e) {
			throw new UnexpectedErrorException(
					"Failed to send message: " + e.getMessage() + " (" + e.getClass().getSimpleName() + ")", e);
		} catch (GroupNotFoundException | NotAGroupMemberException | GroupSendingNotAllowedException e) {
			throw new UserErrorException(e.getMessage());
		}
		}
		catch(Exception ex) {
			ex.printStackTrace(); 
		}
		
	}

	@Override
	public void verify(String code) {
		try {
			registrationManager = RegistrationManager.initInternal(userName);
			registrationManager.verifyAccount(code, null);
			registrationManager.close();
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
	
	 private void outputResult(final OutputWriter outputWriter, final long timestamp) {
	        if (outputWriter instanceof PlainTextWriter) {
	            final var writer = (PlainTextWriter) outputWriter;
	            writer.println("{}", timestamp);
	        } else {
	            final var writer = (JsonWriter) outputWriter;
	            writer.write(Map.of("timestamp", timestamp));
	        }
	    }

}