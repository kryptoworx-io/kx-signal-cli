package org.asamk.signal;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
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
import org.asamk.signal.manager.UntrustedIdentityException;
import org.asamk.signal.manager.api.Message;
import org.asamk.signal.manager.api.RecipientIdentifier;
import org.asamk.signal.manager.config.ServiceConfig;
import org.asamk.signal.manager.config.ServiceEnvironment;
import org.asamk.signal.manager.groups.GroupNotFoundException;
import org.asamk.signal.manager.groups.GroupSendingNotAllowedException;
import org.asamk.signal.manager.groups.NotAGroupMemberException;
import org.asamk.signal.manager.storage.SignalAccount;
import org.asamk.signal.manager.storage.identities.TrustNewIdentity;
import org.asamk.signal.util.CommandUtil;
import org.asamk.signal.util.ErrorUtils;
import org.asamk.signal.util.IOUtils;
import org.asamk.signal.util.SecurityProvider;
import org.bouncycastle.asn1.x509.Time;
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
	public void register(final String phoneNumber, Boolean verifyVoice) {
		System.out.println("in register for number: " + phoneNumber);

		try {
			registrationManager = RegistrationManager.initInternal(userName);
			registrationManager.register(verifyVoice, null);
			registrationManager.close();
			registrationManager = null;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void send(final String message, final String targetNumber, final String attachmentFileName) {

		try {
			System.out.println("in sending message: ");

			if (generalManager == null) {
				generalManager = loadManager(userName, getDefaultDataPath(), ServiceEnvironment.LIVE,
						TrustNewIdentity.ON_FIRST_USE);
			}
			var m = generalManager;

			final var recipientStrings = Collections.singletonList(targetNumber);

			final Set<RecipientIdentifier> recipientIdentifiers = CommandUtil.getRecipientIdentifiers(m, false,
					recipientStrings, Collections.EMPTY_LIST);

			final var isEndSession = false;
			if (isEndSession) {
				final Set singleRecipients = recipientIdentifiers.stream()
						.filter(r -> r instanceof RecipientIdentifier.Single)
						.map(RecipientIdentifier.Single.class::cast).collect(Collectors.toSet());
				if ((singleRecipients).isEmpty()) {
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

			List<String> attachments = attachmentFileName == null ? Collections.emptyList()
					: Collections.singletonList(attachmentFileName);
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
		} catch (Exception ex) {
			// Catch checked exceptions from the underlying API and
			// re-throw as runtime exception.
			throw new RuntimeException(ex.getMessage());
		} finally {
			closeManager();
		}
	}

	@Override
	public void verify(String code) {
		try {
			registrationManager = RegistrationManager.initInternal(userName);
			registrationManager.verifyAccount(code, null);
			registrationManager.close();
			registrationManager = null;
			System.out.println("Verified");
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

	void closeManager() {
		if (generalManager != null) {
			try {
				generalManager.close();
				generalManager = null;
			} catch (IOException ioex) {

			}
		}
	}

	@Override
	public List<String> getRegisteredPhoneNumbers() {
		var registeredUsers = new ArrayList<String>();
		var usernames = Manager.getAllLocalUsernames(getDefaultDataPath());
		if (usernames != null) {
			usernames.forEach(u -> {
				if (SignalAccount.userExists(PathConfig.createDefault(getDefaultDataPath()).getDataPath(), u)) {
					registeredUsers.add(u);
				}
			});

			return registeredUsers;

		} else {
			return Collections.emptyList();
		}

	}

	@Override
	public void receive() {
		double timeout = 1;
		var returnOnTimeout = true;
		if (timeout < 0) {
			returnOnTimeout = false;
			timeout = 3600;
		}
		boolean ignoreAttachments = false;
		try {
			if (generalManager == null) {
				generalManager = loadManager(userName, getDefaultDataPath(), ServiceEnvironment.LIVE,
						TrustNewIdentity.ON_FIRST_USE);
			}

		} catch (CommandException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		var m = generalManager;
		try {
			var outputWriter = new PlainTextWriterImpl(System.out);
			final var handler = new ReceiveMessageHandler(m, (PlainTextWriter) outputWriter);
			m.receiveMessages((long) (timeout * 1000), TimeUnit.MILLISECONDS, returnOnTimeout, ignoreAttachments,
					handler);
			System.out.println("Receive done.");
		} catch (IOException e) {

		} finally {
			closeManager();
		}

	}

	@Override
	public void sendReceipt(String recipientNumber) {
		final var recipientString = recipientNumber;
		final var ts = new Date().getTime();
		final var targetTimestamps = Collections.singletonList(1632219260474L);
		final var type = "read";

		try {
			generalManager = loadManager(userName, getDefaultDataPath(), ServiceEnvironment.LIVE,
					TrustNewIdentity.ON_FIRST_USE);
			final var recipient = CommandUtil.getSingleRecipientIdentifier(recipientString, generalManager.getUsername());

			if (type == null || "read".equals(type)) {
				generalManager.sendReadReceipt(recipient, targetTimestamps);
			} else if ("viewed".equals(type)) {
				generalManager.sendViewedReceipt(recipient, targetTimestamps);
			} else {
				throw new UserErrorException("Unknown receipt type: " + type);
			}

			
		} catch (CommandException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UntrustedIdentityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			closeManager();
		}
		


	}

}
