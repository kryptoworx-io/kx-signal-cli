package io.kryptoworx.signalcli.api;

import java.util.List;

public interface SignalCLI {

    void register(String number);
    void registerWithCaptcha(String phoneNumber, String captcha);
    void verify(String code);
    List<String> getRegisteredPhoneNumbers();
    void send(String message, String targetNumber, String attachmentFileName);

}
