package org.asamk.signal;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.asamk.signal.manager.api.SendMessageResults;
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
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.whispersystems.signalservice.api.messages.SignalServiceContent;
import org.whispersystems.signalservice.api.messages.SignalServiceEnvelope;
import org.whispersystems.signalservice.api.messages.SignalServiceReceiptMessage;

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

//			var usernames = Manager.getAllLocalUsernames(getDefaultDataPath());
//			userName = usernames.get(0);
//			
//			generalManager = loadManager(userName, getDefaultDataPath(), ServiceEnvironment.LIVE,
//					TrustNewIdentity.ON_FIRST_USE);

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private File getDefaultDataPath() {
		return new File(IOUtils.getDataHomeDir(), "signal-cli");
	}

	private Manager getManager() throws Exception {
		if (generalManager == null) {
			var usernames = Manager.getAllLocalUsernames(getDefaultDataPath());
			userName = usernames.get(0);
			generalManager = loadManager(userName, getDefaultDataPath(), ServiceEnvironment.LIVE,
					TrustNewIdentity.ON_FIRST_USE);
		}

		return generalManager;
	}

	private Manager loadManager(final String username, final File dataPath, final ServiceEnvironment serviceEnvironment,
			final TrustNewIdentity trustNewIdentity) throws CommandException {
		Manager manager;
		try {
			manager = Manager.init(username, dataPath, serviceEnvironment, BaseConfig.USER_AGENT, trustNewIdentity, null);
		} catch (NotRegisteredException e) {
			throw new RuntimeException("User " + username + " is not registered.");
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
			registrationManager = RegistrationManager.initInternal(userName, null);
			registrationManager.register(verifyVoice, null);
			registrationManager.close();
			registrationManager = null;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public SendMessageResults send(final String message, final String targetNumber, final String attachmentFileName) {

		try {
			System.out.println("in sending message: ");

			var m = getManager();
			
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
					return null;
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
				// m.close();
				ErrorUtils.handleSendMessageResults(results.getResults());
				return results;
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
		}
	}

	@Override
	public void verify(String code) throws Exception  {
		
			registrationManager = RegistrationManager.initInternal(userName, null);
			registrationManager.verifyAccount(code, null);
			registrationManager.close();
			registrationManager = null;

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

	void closeManager(Manager m) {
		if (m != null) {
			try {
				m.close();
			} catch (IOException ioex) {
				throw new RuntimeException(ioex);
			}
		}
	}

	@Override
	public List<String> getRegisteredPhoneNumbers() {
		var registeredUsers = new ArrayList<String>();
		var usernames = Manager.getAllLocalUsernames(getDefaultDataPath());
		var dataPath = PathConfig.createDefault(getDefaultDataPath()).getDataPath();
		if (usernames != null) {
			usernames.forEach(u -> {
				if (SignalAccount.userExists(dataPath, u)) {

					try (var account = SignalAccount.load(dataPath, u, true, TrustNewIdentity.ON_FIRST_USE)) {

						if (account.isRegistered()) {
					registeredUsers.add(u);
				}

					} catch (IOException ex) {
						// TODO
						// swallow for now, but decide on what to do with all the exception thrown from
						// signalcli.

					}
				}
			});
			return registeredUsers;
		} else {
			return Collections.emptyList();
		}

	}

	@Override
	public List<SignalServiceReceiptMessage> receive() {
		double timeout = 1;
		var returnOnTimeout = true;
		if (timeout < 0) {
			returnOnTimeout = false;
			timeout = 3600;
		}
		boolean ignoreAttachments = false;
		
		try {
			var m = getManager();
			var outputWriter = new PlainTextWriterImpl(System.out);
			final var receiptMessages = new ArrayList<SignalServiceReceiptMessage>();
			final var handler = new ReceiveMessageHandler(m, (PlainTextWriter) outputWriter) {

				@Override
				public void handleMessage(SignalServiceEnvelope envelope, SignalServiceContent content,
						Throwable exception) {
					super.handleMessage(envelope, content, exception);
					try {
						if (content != null) {
							if (content.getReceiptMessage().isPresent()) {
								writer.println("Received a receipt message");
								var receiptMessage = content.getReceiptMessage().get();
								receiptMessages.add(receiptMessage);
							}
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			m.receiveMessages((long) (timeout * 1000), TimeUnit.MILLISECONDS, returnOnTimeout, ignoreAttachments,
					handler);
			System.out.println("Receive done.");
			return receiptMessages;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

	@Override
	public void sendReceipt(String recipientNumber, long messageTimestamp) {
		final var recipientString = recipientNumber;
		final var targetTimestamps = Collections.singletonList(messageTimestamp);
		final var type = "read";

		try {
			var m = getManager();
			final var recipient = CommandUtil.getSingleRecipientIdentifier(recipientString, m.getUsername());

			if (type == null || "read".equals(type)) {
				m.sendReadReceipt(recipient, targetTimestamps);
			} else if ("viewed".equals(type)) {
				m.sendViewedReceipt(recipient, targetTimestamps);
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
		} catch (Exception e) {
			// TODO handle
			e.printStackTrace();
		}

	}

	@Override
	public void close() throws IOException {
		if (generalManager != null) {
			generalManager.close();
		}
		
		if (registrationManager != null) {
			registrationManager.close();
		}
		
	}

}
