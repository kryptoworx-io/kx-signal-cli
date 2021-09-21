package org.asamk.signal.manager.storage.stickers;

public interface IStickerStore {

    Sticker getSticker(StickerPackId packId);

    void updateSticker(Sticker sticker);

}