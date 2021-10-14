package io.kryptoworx.signalcli.api;

import java.io.Closeable;
import java.util.List;

import org.asamk.signal.manager.api.SendMessageResults;
import org.whispersystems.signalservice.api.messages.SignalServiceReceiptMessage;

public interface SignalCLI extends Closeable {

    void register(String number, Boolean voiceVerify, String name);
    void registerWithCaptcha(String phoneNumber, String captcha);
    void verify(String code, String profileName) throws Exception;
    SendMessageResults send(String message, String targetNumber, String attachmentFileName);
    void sendReceipt(String receiptNumber, long messageTimestamp);
    List<SignalServiceReceiptMessage> receive();
	String getPhoneNumber();
}
