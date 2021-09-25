package org.asamk.signal.manager.storage.recipients;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.asamk.signal.manager.storage.sessions.SessionStore;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.whispersystems.libsignal.SignalProtocolAddress;
import org.whispersystems.libsignal.state.SessionRecord;
import org.whispersystems.signalservice.api.push.SignalServiceAddress;

public class SessionStoreTest extends H2Test {

	private TestDataSource dataSource;
	private SessionStore store;
	
	private static final UUID[] RECIPIENT_IDS = new UUID[] {
		UUID.fromString("06d6bea8-4a00-41de-8aa2-b40ae6f9e804"),
		UUID.fromString("821608c6-8f21-4d39-98f3-f4794da1455c"),
		UUID.fromString("4b9e620d-e902-40d0-81d1-fec10d9c6c56")
	};
	
	private static final String[] RECIPIENT_NUMBERS = new String[] {
			"+280000000001",
			"+280000000002",
			"+280000000003"
	};
	
	private final RecipientResolver recipientResolver = new RecipientResolver() {
		
		@Override
		public RecipientId resolveRecipient(UUID uuid) {
			int i = indexOf(RECIPIENT_IDS, uuid);
			return i >= 0 ? RecipientId.of(i + 1) : null;
		}
		
		@Override
		public RecipientId resolveRecipient(SignalServiceAddress address) {
			return address.getUuid() != null ? resolveRecipient(address.getUuid()) : null;
		}
		
		@Override
		public RecipientId resolveRecipient(RecipientAddress address) {
			Optional<UUID> uuid = address.getUuid();
			Optional<String> e164 = address.getNumber();
			if (uuid.isPresent()) {
				return resolveRecipient(uuid.get());
			} else if (e164.isPresent()) {
				return resolveNumber(e164.get());
			} else {
				return null;
			}
		}
		
		@Override
		public RecipientId resolveRecipient(String identifier) {
			try {
				UUID uuid = UUID.fromString(identifier);
				return resolveRecipient(uuid);
			} catch (IllegalArgumentException e) {
				// 
			}
			return resolveNumber(identifier);
		}
		
		private RecipientId resolveNumber(String number) {
			int i = indexOf(RECIPIENT_NUMBERS, number);
			return i >= 0 ? RecipientId.of(i + 1) : null;
		}
		
		private static <T> int indexOf(T[] array, T element) {
			for (int i = 0; i < array.length; i++) {
				if (array[i].equals(element)) {
					return i;
				}
			}
			return -1;
		}
	};
	
	@Before
	public void setup() throws IOException {
		dataSource = createDataSource();
		store = new SessionStore(dataSource, recipientResolver);
	}
	
	@After
	public void cleanup() throws IOException {
		dataSource.close();
	}
	
	
	@Test
	public void test1() {
		SignalProtocolAddress addr1_1 = new SignalProtocolAddress(RECIPIENT_NUMBERS[0], 1);
		SignalProtocolAddress addr1_2 = new SignalProtocolAddress(RECIPIENT_NUMBERS[0], 2);
		SignalProtocolAddress addr1_8 = new SignalProtocolAddress(RECIPIENT_NUMBERS[0], 8);

		store.storeSession(addr1_1, new SessionRecord());
		store.storeSession(addr1_2, new SessionRecord());
		store.storeSession(addr1_8, new SessionRecord());
		
		Assert.assertTrue(setEquals(store.getSubDeviceSessions(RECIPIENT_NUMBERS[0]), 2, 8));
		Assert.assertTrue(store.getSubDeviceSessions(RECIPIENT_NUMBERS[1]).isEmpty());

		store.mergeRecipients(RecipientId.of(2), RecipientId.of(1));
		
		Assert.assertTrue(store.getSubDeviceSessions(RECIPIENT_NUMBERS[0]).isEmpty());
		Assert.assertTrue(setEquals(store.getSubDeviceSessions(RECIPIENT_NUMBERS[1]), 2, 8));

		store.archiveAllSessions();
		
		store.deleteAllSessions(RecipientId.of(1));
		Assert.assertTrue(setEquals(store.getSubDeviceSessions(RECIPIENT_NUMBERS[1]), 2, 8));
		store.deleteAllSessions(RECIPIENT_NUMBERS[1]);
		Assert.assertTrue(store.getSubDeviceSessions(RECIPIENT_NUMBERS[1]).isEmpty());		
	}
	
	@SuppressWarnings("unchecked")
	private static <T> boolean setEquals(Collection<T> c, T... elements) {
		Set<T> s1 = c instanceof Set ? (Set<T>) c : Set.copyOf(c);
		Set<T> s2 = Set.of(elements); 
		return s1.equals(s2);
	}
}
