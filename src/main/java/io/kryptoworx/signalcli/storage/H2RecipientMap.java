package io.kryptoworx.signalcli.storage;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import javax.sql.DataSource;

import org.asamk.signal.manager.storage.recipients.Contact;
import org.asamk.signal.manager.storage.recipients.Profile;
import org.asamk.signal.manager.storage.recipients.Profile.Capability;
import org.asamk.signal.manager.storage.recipients.Profile.UnidentifiedAccessMode;
import org.asamk.signal.manager.storage.recipients.Recipient;
import org.asamk.signal.manager.storage.recipients.RecipientAddress;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.profiles.ProfileKey;
import org.signal.zkgroup.profiles.ProfileKeyCredential;
import org.whispersystems.libsignal.util.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.kryptoworx.signalcli.storage.proto.Storage.PbContact;
import io.kryptoworx.signalcli.storage.proto.Storage.PbProfile;
import io.kryptoworx.signalcli.storage.proto.Storage.PbRecipient;
import io.kryptoworx.signalcli.storage.proto.Storage.PbUUID;


public class H2RecipientMap extends H2Map<Long, Recipient> implements AutoCloseable {

	private Column<UUID> uuidColumn;
	private Column<String> e164Column;
	
	public H2RecipientMap(DataSource dataSource) throws IOException {
		super(dataSource, "recipients", H2RecipientMap::serialize, H2RecipientMap::deserialize);
	}
	
	@Override
	protected Column<Long> createPrimaryKeyColumn() {
		return new Column<>("id", "BIGINT", PreparedStatement::setLong, ResultSet::getLong);
	}
	
	@Override
	protected void createTableAndIndices(Connection connection) throws SQLException {
		super.createTableAndIndices(connection);
		String sqlCreateSequence = "CREATE SEQUENCE IF NOT EXISTS sq_recipients START WITH 1";
		try (PreparedStatement stmt = connection.prepareStatement(sqlCreateSequence)) {
			stmt.execute();
		}
	}
	
	@Override
	protected Column<?>[] createIndexColumns() {
		return new Column<?>[] {
			e164Column = new Column<>("e164", "VARCHAR", PreparedStatement::setString, ResultSet::getString),
			uuidColumn = new Column<UUID>("guid", "UUID", PreparedStatement::setObject, (rs, i) -> (UUID) rs.getObject(i)),
			new Column<>("has_contact", "BOOLEAN", PreparedStatement::setBoolean, ResultSet::getBoolean)
		};
	}
	
	public Recipient get(RecipientId recipientId) {
		return get(recipientId.getId());
	}
	
	public Recipient get(UUID uuid) {
		return get(uuidColumn, uuid);
	}

	public Recipient get(String e164) {
		return get(e164Column, e164);
	}
	
	public void put(RecipientId recipientId, Recipient recipient) {
		RecipientAddress address = recipient.getAddress();
		String e164 = address.getNumber().orElse(null);
		UUID uuid = address.getUuid().orElse(null);
		put(recipientId.getId(), recipient, e164, uuid, recipient.getContact() != null);
	}
	
	public void remove(RecipientId recipientId) {
		remove(recipientId.getId());
	}
	
	public List<Pair<RecipientId, Contact>> getContacts() {
		return transaction(this::dbGetContacts);
	}
	
	private List<Pair<RecipientId, Contact>> dbGetContacts(Connection connection) throws SQLException {
		List<Pair<RecipientId, Contact>> result = new ArrayList<>();
		String sqlQuery = "SELECT id, content FROM recipients WHERE has_contact";
		try (PreparedStatement stmt = connection.prepareStatement(sqlQuery);
				ResultSet rs = stmt.executeQuery()) {
			while (rs.next()) {
				Recipient r = deserialize(rs.getLong(1), rs.getBytes(2));
				result.add(new Pair<>(r.getRecipientId(), r.getContact()));
			}
		}
		return result;
	}
	
	public long nextRecipientId() {
		return transaction(this::dbNextRecipientId);
	}
	
	private long dbNextRecipientId(Connection connection) throws SQLException {
		String sqlQuery = "SELECT NEXT VALUE FOR sq_recipients";
		try (PreparedStatement stmt = connection.prepareStatement(sqlQuery);
			ResultSet rs = stmt.executeQuery()) {
			rs.next();
			return rs.getLong(1);
		}
	}

	private static Recipient deserialize(long id, byte[] recipientBytes) {
		PbRecipient r = parseFrom(recipientBytes);
		Contact contact = null;
		Profile profile = null;
		ProfileKey profileKey = null;
		ProfileKeyCredential profileKeyCredential = null;
		
		PbContact pbContact = r.getContact();
		if (pbContact != null) {
			contact = new Contact(pbContact.getName(),
					pbContact.getColor(),
					pbContact.getMessageExpirationTime(),
					pbContact.getBlocked(),
					pbContact.getArchived());
        }

		if (r.getProfileKey() != null) {
            try {
                profileKey = new ProfileKey(r.getProfileKey().toByteArray());
            } catch (InvalidInputException ignored) {
            	
            }
        }

        if (r.getProfileKeyCredential() != null) {
            try {
                profileKeyCredential = new ProfileKeyCredential(r.getProfileKeyCredential().toByteArray());
            } catch (Throwable ignored) {
            	
            }
        }
        
        if (r.hasProfile()) {
        	PbProfile pbProfile = r.getProfile();
        	var pbCapabilities = pbProfile.getCapabilitiesValueList();
        	Set<Capability> capabilities = EnumSet.noneOf(Capability.class);
        	if (pbCapabilities != null) {
        		for (int c : pbCapabilities) {
        			capabilities.add(fromOrdinal(Capability.class, c));
        		}
        	}
            profile = new Profile(pbProfile.getLastUpdateTimestamp(),
                    pbProfile.getGivenName(),
                    pbProfile.getFamilyName(),
                    pbProfile.getAbout(),
                    pbProfile.getAboutEmoji(),
                    fromOrdinal(UnidentifiedAccessMode.class, pbProfile.getUnidentifiedAccessModeValue()),
                    capabilities);
        }

        Optional<UUID> uuid = r.hasUuid() 
        		? Optional.of(new UUID(r.getUuid().getMsb(), r.getUuid().getLsb())) 
				: Optional.empty();
        Optional<String> e164 = Optional.ofNullable(r.getNumber());
        return new Recipient(RecipientId.of(r.getId()), 
        		new RecipientAddress(uuid, e164), 
        		contact, 
        		profileKey, 
        		profileKeyCredential,
        		profile);
	}
	
	private static <E extends Enum<E>> E fromOrdinal(Class<E> enumClass, int ordinal) {
		E[] values = enumClass.getEnumConstants();
		if (ordinal < 0 || ordinal >= values.length) {
			throw new IllegalArgumentException();
		}
		return values[ordinal];
	}
	
	private static PbRecipient parseFrom(byte[] recipientBytes) {
		try {
			return PbRecipient.parseFrom(recipientBytes);
		} catch (InvalidProtocolBufferException e) {
			throw new AssertionError("Failed to decode recipient", e);
		}
	}
    
	private static byte[] serialize(long id, Recipient recipient) {
    	var recipientBuilder = PbRecipient.newBuilder()
    		.setId(id)
    		;
    	
    	Contact contact = recipient.getContact();
    	if (contact != null) {
    		recipientBuilder.setContact(PbContact.newBuilder()
    				.setName(contact.getName())
    				.setColor(contact.getColor())
    				.setMessageExpirationTime(contact.getMessageExpirationTime())
    				.setBlocked(contact.isBlocked())
    				.setArchived(contact.isArchived()));
    	}
    	
    	Profile profile = recipient.getProfile();
    	if (profile != null) {
    		var profileBuilder = PbProfile.newBuilder()
    				.setLastUpdateTimestamp(profile.getLastUpdateTimestamp())
    				.setUnidentifiedAccessModeValue(profile.getUnidentifiedAccessMode().ordinal());
    		Optional.ofNullable(profile.getGivenName()).ifPresent(profileBuilder::setGivenName);
    		Optional.ofNullable(profile.getFamilyName()).ifPresent(profileBuilder::setFamilyName);
    		Optional.ofNullable(profile.getAbout()).ifPresent(profileBuilder::setAbout);
    		Optional.ofNullable(profile.getAboutEmoji()).ifPresent(profileBuilder::setAboutEmoji);
    		for (var c : profile.getCapabilities()) {
    			profileBuilder.addCapabilitiesValue(c.ordinal());
    		}
    		recipientBuilder.setProfile(profileBuilder);
    	}
    	
    	recipient.getAddress().getUuid()
    			.map(uuid -> PbUUID.newBuilder().setLsb(uuid.getLeastSignificantBits()).setMsb(uuid.getMostSignificantBits()))
    			.ifPresent(recipientBuilder::setUuid);
    	recipient.getAddress().getNumber().ifPresent(recipientBuilder::setNumber);
    	Optional.ofNullable(recipient.getProfileKey())
    			.map(ProfileKey::serialize)
    			.map(ByteString::copyFrom)
    			.ifPresent(recipientBuilder::setProfileKey);
    	Optional.ofNullable(recipient.getProfileKeyCredential())
    			.map(ProfileKeyCredential::serialize)
    			.map(ByteString::copyFrom)
    			.ifPresent(recipientBuilder::setProfileKeyCredential);
    	return recipientBuilder.build().toByteArray();
    }
}
