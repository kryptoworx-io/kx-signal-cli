package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.asamk.signal.manager.groups.GroupId;
import org.asamk.signal.manager.groups.GroupIdV1;
import org.asamk.signal.manager.groups.GroupIdV2;
import org.asamk.signal.manager.groups.GroupUtils;
import org.asamk.signal.manager.storage.groups.GroupInfo;
import org.asamk.signal.manager.storage.groups.GroupInfoV1;
import org.asamk.signal.manager.storage.groups.GroupInfoV2;
import org.asamk.signal.manager.storage.groups.IGroupStore;
import org.asamk.signal.manager.storage.recipients.RecipientId;
import org.asamk.signal.manager.storage.recipients.RecipientResolver;
import org.signal.storageservice.protos.groups.local.DecryptedGroup;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.groups.GroupMasterKey;

import com.google.protobuf.InvalidProtocolBufferException;

public class HsqlGroupStore extends HsqlStore implements IGroupStore {

    private static final String SQL_QUERY_V1 = """
            SELECT 
                g.id,
                g.id_v2,
                g.name,
                g.color,
                g.msg_expiration_time,
                g.blocked,
                g.archived,
                ARRAY_AGG(m.recipient)
            FROM group_v1 g
            LEFT JOIN group_v1_member m ON m.group_id = g.id
            """;
    private static final String SQL_QUERY_V2 = """
            SELECT 
                id,
                master_key,
                blocked,
                content
            FROM group_v2 g
            """;

    
    private final RecipientResolver recipientResolver;
    private final String account;

    public HsqlGroupStore(SQLConnectionFactory connectionFactory, String account, RecipientResolver recipientResolver) {
        super(connectionFactory);
        this.recipientResolver = recipientResolver;
        this.account = account;
        voidTransaction(this::initialize);
    }
    
    private void initialize(Connection connection) throws SQLException {
        String sqlCreateTableGroupV1 = """
                CREATE TABLE IF NOT EXISTS group_v1
                (
                    id BINARY(16) NOT NULL PRIMARY KEY,
                    id_v2 BINARY(32),
                    name VARCHAR(30 CHARACTERS),
                    color VARCHAR(20 CHARACTERS),
                    msg_expiration_time INT,
                    blocked BOOLEAN,
                    archived BOOLEAN,
                    account VARCHAR(20 CHARACTERS) REFERENCES account(e164) ON DELETE CASCADE
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlCreateTableGroupV1)) {
            stmt.execute();
        }
        
        String sqlCreateTableGroupV1Member = """
                CREATE TABLE IF NOT EXISTS group_v1_member
                (
                    group_id BINARY(16) NOT NULL REFERENCES group_v1 (id) ON DELETE CASCADE,
                    recipient BIGINT NOT NULL,
                    PRIMARY KEY (group_id, recipient)
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlCreateTableGroupV1Member)) {
            stmt.execute();
        }
        
        String sqlCreateIndexGroupV1IdV2 = "CREATE INDEX IF NOT EXISTS ix_groupv1_idv2 ON group_v1 (id_v2)";
        try (PreparedStatement stmt = connection.prepareStatement(sqlCreateIndexGroupV1IdV2)) {
            stmt.execute();
        }
        
        String sqlCreateTableGroupV2 = """
                CREATE TABLE IF NOT EXISTS group_v2
                (
                    id BINARY(32) NOT NULL PRIMARY KEY,
                    master_key BINARY(32),
                    blocked BOOLEAN,
                    content BLOB,
                    account VARCHAR(20 CHARACTERS) REFERENCES account(e164) ON DELETE CASCADE
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlCreateTableGroupV2)) {
            stmt.execute();
        }
    }

    private List<GroupInfo> dbLoadGroups(Connection connection, String account, RecipientResolver recipientResolver) throws SQLException {
        List<GroupInfo> groups = new ArrayList<>();
        String sqlQueryV1 = SQL_QUERY_V1 + " WHERE g.account = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQueryV1)) {
            stmt.setString(1, account);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    groups.add(dbReadGroupV1(rs));
                }
            }
        }
        String sqlQueryV2 = SQL_QUERY_V2 + " WHERE g.account = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQueryV2)) {
            stmt.setString(1, account);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    groups.add(dbReadGroupV2(rs));
                }
            }
        }
        return groups;
    }
    
    private static GroupMasterKey groupMasterKey(byte[] bytes) {
        try {
            return new GroupMasterKey(bytes);
        } catch (InvalidInputException e) {
            throw new AssertionError(e);
        }
    }

    private static DecryptedGroup decryptedGroup(byte[] bytes) {
        try {
            return DecryptedGroup.parseFrom(bytes);
        } catch (InvalidProtocolBufferException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public void updateGroup(GroupInfo group) {
        if (group instanceof GroupInfoV2 groupV2 && groupV2.getGroup() != null) {
            voidTransaction(c -> dbUpdateGroup(c, groupV2));
        }
    }
    
    private void dbUpdateGroup(Connection connection, GroupInfoV2 groupInfo) throws SQLException {
        String sqlUpdate = """
                UPDATE group_v2 SET
                    master_key = ?,
                    blocked = ?,
                    content = ?
                WHERE id = ?
                """;
        GroupMasterKey masterKey = groupInfo.getMasterKey();
        try (PreparedStatement stmt = connection.prepareStatement(sqlUpdate)) {
            int p = 1;
            stmt.setBytes(p++, masterKey != null ? masterKey.serialize() : null);
            stmt.setBoolean(p++, groupInfo.isBlocked());
            stmt.setBytes(p++, groupInfo.getGroup().toByteArray());
            stmt.setBytes(p++, groupInfo.getGroupId().serialize());
        }
    }

    @Override
    public void deleteGroupV1(GroupIdV1 groupIdV1) {
        deleteGroup(groupIdV1);
    }

    @Override
    public void deleteGroup(GroupId groupId) {
        voidTransaction(c -> dbDeleteGroup(c, groupId));
    }
    
    private void dbDeleteGroup(Connection connection, GroupId groupId) throws SQLException {
        String table = groupId instanceof GroupIdV1 ? "group_v1" : "group_v2";
        String sqlDelete = "DELETE FROM " + table + " WHERE id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlDelete)) {
            stmt.setBytes(1, groupId.serialize());
            stmt.executeUpdate();
        }
    }

    @Override
    public GroupInfo getGroup(GroupId groupId) {
        return transaction(c -> dbLoadGroup(c, groupId));
    }
    
    private GroupInfo dbLoadGroup(Connection connection, GroupId groupId) throws SQLException {
        GroupInfo result = null; 
        if (groupId instanceof GroupIdV1 idv1) {
            result = dbLoadGroupInfoV1(connection, idv1);
            if (result == null) {
                result = dbLoadGroupInfoV2(connection, GroupUtils.getGroupIdV2(idv1));
            }
        } else if (groupId instanceof GroupIdV2 idv2) {
            result = dbLoadGroupInfoV2(connection, idv2);
            if (result == null) {
                result = dbLoadGroupInfoV1(connection, idv2);
            }
        }
        return result;
    }

    private GroupInfoV1 dbLoadGroupInfoV1(Connection connection, GroupIdV1 id) throws SQLException {
        String sqlQueryV1 = SQL_QUERY_V1 + " WHERE g.id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQueryV1)) {
            stmt.setBytes(1, id.serialize());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) return null;
                return dbReadGroupV1(rs);
            }
        }
    }

    private GroupInfoV1 dbLoadGroupInfoV1(Connection connection, GroupIdV2 id) throws SQLException {
        String sqlQueryV1 = SQL_QUERY_V1 + " WHERE g.id_v2 = ";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQueryV1)) {
            stmt.setBytes(1, id.serialize());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) return null;
                return dbReadGroupV1(rs);
            }
        }
    }

    private GroupInfoV2 dbLoadGroupInfoV2(Connection connection, GroupIdV2 id) throws SQLException {
        String sqlQueryV2 = SQL_QUERY_V2 + " WHERE g.id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQueryV2)) {
            stmt.setBytes(1, id.serialize());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) return null;
                return dbReadGroupV2(rs);
            }
        }
    }
    
    private GroupInfoV1 dbReadGroupV1(ResultSet rs) throws SQLException {
        int c = 1;
        GroupIdV1 id = GroupId.v1(rs.getBytes(c++));
        byte[] idV2Bytes = rs.getBytes(c++);
        GroupIdV2 idV2 = idV2Bytes != null ? GroupId.v2(idV2Bytes) : null;
        String name = rs.getString(c++);
        String color = rs.getString(c++);
        int messageExpTime = rs.getInt(c++);
        boolean blocked = rs.getBoolean(c++);
        boolean archived = rs.getBoolean(c++);
        Set<RecipientId> members = new HashSet<>();
        
        java.sql.Array rsMembers = rs.getArray(c++);
        if (rsMembers != null) {
            if (rsMembers.getArray() instanceof Object[] memberArray) {
                for (Object o : memberArray) {
                    if (o instanceof Long memberId) {
                        members.add(RecipientId.of(memberId));
                    }
                }
            }
        }
        return new GroupInfoV1(id, idV2, name, members, color, messageExpTime, blocked, archived);
    }
    
    private GroupInfoV2 dbReadGroupV2(ResultSet rs) throws SQLException {
        int c = 1;
        GroupIdV2 id = GroupId.v2(rs.getBytes(c++));
        byte[] masterKeyBytes = rs.getBytes(c++);
        GroupMasterKey masterKey = masterKeyBytes != null ? groupMasterKey(masterKeyBytes) : null; 
        boolean blocked = rs.getBoolean(c++);
        byte[] content = rs.getBytes(c++);
        GroupInfoV2 groupInfo = new GroupInfoV2(id, masterKey, blocked);
        groupInfo.setGroup(decryptedGroup(content), recipientResolver);
        return groupInfo; 
    }

    @Override
    public GroupInfoV1 getOrCreateGroupV1(GroupIdV1 groupId) {
        GroupInfo group = transaction(c -> dbLoadGroup(c, groupId));
        if (group instanceof GroupInfoV1 groupV1) {
            return groupV1;
        } else if (group == null) {
            return new GroupInfoV1(groupId);
        } else {
            return null;
        }
    }

    @Override
    public List<GroupInfo> getGroups() {
        return transaction(c -> dbLoadGroups(c, account, recipientResolver));
    }

    @Override
    public void mergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException {
        String sqlUpdate = "UPDATE group_v1_member SET recipient = ? WHERE recipient = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlUpdate)) {
            stmt.setLong(1, recipientId);
            stmt.setLong(2, toBeMergedRecipientId);
            stmt.executeUpdate();
        }
    }
}
