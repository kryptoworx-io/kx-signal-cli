package io.kryptoworx.signalcli.api;


public interface SignalCLI {

    void register(String number);
    void registerWithCaptcha(String phoneNumber, String captcha);
    void verify(String code);
    void send(String message, String targetNumber, String attachmentFileName);

}
