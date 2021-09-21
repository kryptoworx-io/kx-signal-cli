package org.asamk.signal.manager.storage.groups;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.asamk.signal.manager.groups.GroupId;
import org.asamk.signal.manager.groups.GroupIdV1;
import org.asamk.signal.manager.storage.recipients.RecipientId;

public interface IGroupStore {

    void updateGroup(GroupInfo group);

    void deleteGroupV1(GroupIdV1 groupIdV1);

    void deleteGroup(GroupId groupId);

    GroupInfo getGroup(GroupId groupId);

    GroupInfoV1 getOrCreateGroupV1(GroupIdV1 groupId);

    List<GroupInfo> getGroups();

    default void mergeRecipients(RecipientId recipientId, RecipientId toBeMergedRecipientId) { }
    default void mergeRecipients(Connection connection, long recipientId, long toBeMergedRecipientId) throws SQLException { }

}