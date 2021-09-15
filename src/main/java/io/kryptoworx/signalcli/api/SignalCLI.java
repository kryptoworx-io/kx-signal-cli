package io.kryptoworx.signalcli.api;


public interface SignalCLI {

	void init(String number) throws Exception;
    void register(String number);
    void registerWithCaptcha(String phoneNumber, String captcha);
    void verify(String code);
    void send(String message, String targetNumber);

}
