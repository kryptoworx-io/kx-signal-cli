package org.asamk.signal.manager.storage.recipients;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import org.asamk.signal.manager.storage.recipients.Profile.UnidentifiedAccessMode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.signal.zkgroup.internal.ByteArray;
import org.signal.zkgroup.profiles.ProfileKey;

@FixMethodOrder(NAME_ASCENDING)
public class RecipientStoreTest extends H2Test {
	
	private static RecipientStore store;
	private static TestDataSource dataSource;
	
	@BeforeClass
	public static void setup() throws IOException {
		dataSource = createDataSource();
		store = RecipientStore.load(dataSource, (id1, id2) -> {}, 0);
	}
	
	@AfterClass
	public static void tearDown() throws IOException, SQLException {
		store.close();
		dataSource.close();
	}
	
	@Test
    public void test1() throws Exception {
		assertTrue(store.isEmpty());

		UUID uuid = UUID.randomUUID();
		String e164 = "+43681%06d".formatted(random.nextInt(100_000_000)); 
		
		RecipientAddress address = new RecipientAddress(Optional.of(uuid), Optional.of(e164));
		RecipientId recipientId = store.resolveRecipient(address);
		assertNotNull(recipientId);
		assertEquals(recipientId.getId(), 1);
		
		Contact contact = new Contact("Max Mustermann", "orange", random.nextInt(), false, false);
		store.storeContact(recipientId, contact);
		Contact contactFromStore = store.getContact(recipientId);
		assertTrue(equals(contact, contactFromStore));
		
        var contacts = store.getContacts();
        assertEquals(contacts.size(), 1);
        assertEquals(contacts.get(0).first().getId(), 1);
        assertTrue(equals(contacts.get(0).second(), contact));
		
		byte[] profileKeyBytes = new byte[32];
		random.nextBytes(profileKeyBytes);
		ProfileKey profileKey = new ProfileKey(profileKeyBytes);
		store.storeProfileKey(recipientId, profileKey);
		ProfileKey profileKeyFromStore = store.getProfileKey(recipientId);
		assertTrue(equals(profileKey, profileKeyFromStore));
		
		/*
		byte[] profileKeyCredentialBytes = new byte[145];
		random.nextBytes(profileKeyCredentialBytes);
        ProfileKeyCredential profileKeyCredential = new ProfileKeyCredential(profileKeyCredentialBytes);
        store.storeProfileKeyCredential(recipientId, profileKeyCredential);
        ProfileKeyCredential profileKeyCredentialFromStore = store.getProfileKeyCredential(recipientId);
        assertTrue(equals(profileKeyCredential, profileKeyCredentialFromStore));
        */
        
        Profile profile = new Profile(System.currentTimeMillis(),
                "Maximilian",
                "Mustermann",
                "Lorem ipsum dolor sit amet",
                "xxxxxxxxxxxxxxxxxxxx",
                UnidentifiedAccessMode.UNKNOWN,
                EnumSet.allOf(Profile.Capability.class));
        store.storeProfile(recipientId, profile);
        Profile profileFromStore = store.getProfile(recipientId);
        assertTrue(equals(profile, profileFromStore));
        
        assertFalse(store.isEmpty());
        
        
        /*
        Recipient r = new Recipient(id, address, contact, profileKey, profileKeyCredential, profile);
        byte[] pbBytes = toProtobuf(r);
        System.out.println(pbBytes.length);
        recipients.put(id.getId(), pbBytes);
        byUUID.put(r.getAddress().getUuid().get(),   (long) i);
        byE164.put(r.getAddress().getNumber().get(), (long) i);
        System.exit(0);
		 */
	}
	
	@Test
	public void test2() throws Exception {
		UUID uuid = UUID.randomUUID();
		String e164 = "+43681%06d".formatted(random.nextInt(100_000_000));
		
		RecipientAddress addrUuid = new RecipientAddress(uuid, null);
		RecipientAddress addrE164 = new RecipientAddress(null, e164);
		RecipientAddress addrBoth = new RecipientAddress(uuid, e164);
		
		RecipientId idUuid = store.resolveRecipient(addrUuid);
		RecipientId idE164 = store.resolveRecipient(addrE164);
		assertEquals(idE164.getId(), idUuid.getId() + 1);
		
		RecipientId idBoth = store.resolveRecipientTrusted(addrBoth);
		assertEquals(idBoth.getId(), idUuid.getId());
		
		Recipient r1 = store.getRecipient(idUuid);
		Recipient r2 = store.getRecipient(idE164);
		assertEquals(r1.getRecipientId(), r2.getRecipientId());
	}

	@Test
	public void test3() throws Exception {
		for (int i = 0; i < 10_000; i++) {
			UUID uuid = UUID.randomUUID();
			String e164 = "+43681%06d".formatted(random.nextInt(100_000_000)); 
			
			RecipientAddress address = new RecipientAddress(Optional.of(uuid), Optional.of(e164));
			RecipientId recipientId = store.resolveRecipient(address);
			
			Contact contact = new Contact("Max Mustermann", "orange", random.nextInt(), false, false);
			store.storeContact(recipientId, contact);
			
			byte[] profileKeyBytes = new byte[32];
			random.nextBytes(profileKeyBytes);

			ProfileKey profileKey = new ProfileKey(profileKeyBytes);
			store.storeProfileKey(recipientId, profileKey);
			
			/*
			byte[] profileKeyCredentialBytes = new byte[145];
			random.nextBytes(profileKeyCredentialBytes);
	        ProfileKeyCredential profileKeyCredential = new ProfileKeyCredential(profileKeyCredentialBytes);
	        store.storeProfileKeyCredential(recipientId, profileKeyCredential);
	        ProfileKeyCredential profileKeyCredentialFromStore = store.getProfileKeyCredential(recipientId);
	        assertTrue(equals(profileKeyCredential, profileKeyCredentialFromStore));
	        */
	        
	        Profile profile = new Profile(System.currentTimeMillis(),
	                "Maximilian",
	                "Mustermann",
	                "Lorem ipsum dolor sit amet",
	                "xxxxxxxxxxxxxxxxxxxx",
	                UnidentifiedAccessMode.UNKNOWN,
	                EnumSet.allOf(Profile.Capability.class));
	        store.storeProfile(recipientId, profile);
		}
	}
	
	private static boolean equals(Contact c1, Contact c2) {
		if (c1 == null) return c2 == null;
		if (c2 == null) return false;
		return Objects.equals(c1.getName(), c2.getName())
				&& Objects.equals(c1.getColor(), c2.getColor())
				&& c1.getMessageExpirationTime() == c2.getMessageExpirationTime()
				&& c1.isBlocked() == c2.isBlocked()
				&& c1.isArchived() == c2.isArchived();
	}

	private static boolean equals(Profile p1, Profile p2) {
		if (p1 == null) return p2 == null;
		if (p2 == null) return false;
		return Objects.equals(p1.getGivenName(), p2.getGivenName())
				&& Objects.equals(p1.getFamilyName(), p2.getFamilyName())
				&& Objects.equals(p1.getAbout(), p2.getAbout())
				&& Objects.equals(p1.getAboutEmoji(), p2.getAboutEmoji())
				&& Objects.equals(p1.getUnidentifiedAccessMode(), p2.getUnidentifiedAccessMode())
				&& Objects.equals(p1.getCapabilities(), p2.getCapabilities())
				&& p1.getLastUpdateTimestamp() == p2.getLastUpdateTimestamp();
	}

	private static boolean equals(ByteArray bs1, ByteArray bs2) {
		if (bs1 == null) return bs2 == null;
		if (bs2 == null) return false;
		return Arrays.equals(bs1.getInternalContentsForJNI(), bs2.getInternalContentsForJNI());
	}

}
