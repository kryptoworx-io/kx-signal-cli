package io.kryptoworx.signalcli.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.asamk.signal.manager.storage.stickers.IStickerStore;
import org.asamk.signal.manager.storage.stickers.Sticker;
import org.asamk.signal.manager.storage.stickers.StickerPackId;

public class HsqlStickerStore extends HsqlStore implements IStickerStore {

    private final Object stickers = new Object();

    public HsqlStickerStore(SQLConnectionFactory connectionFactory) {
        super(connectionFactory);
        voidTransaction(this::initialize);
    }
    
    private void initialize(Connection connection) throws SQLException {
        String sqlCreateTable = """
                CREATE TABLE IF NOT EXISTS sticker
                (
                    pack_id VARBINARY(1024) NOT NULL PRIMARY KEY,
                    pack_key VARBINARY(1024) NOT NULL,
                    installed BOOLEAN NOT NULL
                )
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlCreateTable)) {
            stmt.execute();
        }
    }

    public Sticker getSticker(StickerPackId packId) {
        synchronized (stickers) {
            return transaction(c -> dbLoadSticker(c, packId));
        }
    }
    
    private Sticker dbLoadSticker(Connection connection, StickerPackId packId) throws SQLException {
        String sqlQuery = "SELECT pack_key, installed FROM sticker where pack_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sqlQuery)) {
            stmt.setBytes(1, packId.serialize());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) return null;
                return new Sticker(packId, rs.getBytes(1), rs.getBoolean(2));
            }
        }
    }

    public void updateSticker(Sticker sticker) {
        synchronized (stickers) {
            voidTransaction(c -> dbUpdateSticker(c, sticker));
        }
    }
    
    private void dbUpdateSticker(Connection connection, Sticker sticker) throws SQLException {
        String sqlMerge = """
                MERGE INTO sticker t
                USING (VALUES CAST(? AS VARBINARY(1024)), CAST(? AS VARBINARY(1024)), ?) AS s(pid, pkey, inst)
                ON (s.pid = t.pack_id)
                WHEN MATCHED THEN UPDATE SET t.pack_key = s.pkey, t.installed = s.inst
                WHEN NOT MATCHED THEN INSERT (pack_id, pack_key, installed) VALUES (s.pid, s.pkey, s.inst)
                """;
        try (PreparedStatement stmt = connection.prepareStatement(sqlMerge)) {
            int p = 1;
            stmt.setBytes(p++, sticker.getPackId().serialize());
            stmt.setBytes(p++, sticker.getPackKey());
            stmt.setBoolean(p++, sticker.isInstalled());
            stmt.executeUpdate();
        }
    }
}
