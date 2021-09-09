package io.kryptoworx.signalcli.api;


public interface SignalCLI {

    void register(String number);
    void send(String message, String targetNumber);

}
