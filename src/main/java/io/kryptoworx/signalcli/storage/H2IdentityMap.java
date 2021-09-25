package io.kryptoworx.signalcli.storage;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.List;

import javax.sql.DataSource;

import org.asamk.signal.manager.TrustLevel;
import org.asamk.signal.manager.storage.identities.IdentityInfo;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.whispersystems.libsignal.IdentityKey;
import org.whispersystems.libsignal.InvalidKeyException;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.kryptoworx.signalcli.storage.proto.Storage.PbIdentity;

public class H2IdentityMap extends H2Map<Long, IdentityInfo> {
	
	public H2IdentityMap(DataSource dataSource) {
		super(dataSource, "identities", H2IdentityMap::serialize, H2IdentityMap::deserialize);
	}
	
	@Override
	protected Column<Long> createPrimaryKeyColumn() {
		return new Column<>("id", "BIGINT", PreparedStatement::setLong, ResultSet::getLong);
	}
	
	public List<IdentityInfo> getIdentities() {
		return getAll();
	}
	
	private static byte[] serialize(long recipientId, IdentityInfo identity) {
		return PbIdentity.newBuilder()
				.setIdentityKey(ByteString.copyFrom(identity.getIdentityKey().serialize()))
				.setTrustLevel(identity.getTrustLevel().ordinal())
				.setAddedTimestamp(identity.getDateAdded().getTime())
				.build()
				.toByteArray();
	}
	
	private static IdentityInfo deserialize(long recipientId, byte[] identityBytes) {
		try {
			PbIdentity pbIdentity = PbIdentity.parseFrom(identityBytes);
			return new IdentityInfo(RecipientId.of(recipientId), 
					new IdentityKey(pbIdentity.getIdentityKey().toByteArray()),
					Enums.fromOrdinal(TrustLevel.class, pbIdentity.getTrustLevel()),
					new Date(pbIdentity.getAddedTimestamp()));
		} catch (InvalidProtocolBufferException | InvalidKeyException e) {
			throw new AssertionError("Failed to decode identity", e);
		}
	}
}
