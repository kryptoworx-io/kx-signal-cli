# kx-signal-cli

kx-signal-cli builds on signal-cli (see README_asmk.md) and provides an OSGi plugin compatible API layer to use its functionality.

## Installation in Eclipse
Import as a plugin project.

## Installation on the command line

You can [build signal-cli](#building) yourself, or use the [provided binary files](https://github.com/AsamK/signal-cli/releases/latest), which should work on Linux, macOS and Windows. For Arch Linux there is also a [package in AUR](https://aur.archlinux.org/packages/signal-cli/) and there is a [FreeBSD port](https://www.freshports.org/net-im/signal-cli) available as well.

System requirements:
- at least Java Runtime Environment (JRE) 11
- native libraries: libzkgroup, libsignal-client

  Those are bundled for x86_64 Linux (with recent enough glibc, see #643), for other systems/architectures see: [Provide native lib for libsignal](https://github.com/AsamK/signal-cli/wiki/Provide-native-lib-for-libsignal)

### Publish to local maven repo

```sh
./gradlew publishToMavenLocal
```

### Install system-wide on Linux
See [latest version](https://github.com/AsamK/signal-cli/releases).
```sh
export VERSION=<latest version, format "x.y.z">
wget https://github.com/AsamK/signal-cli/releases/download/v"${VERSION}"/signal-cli-"${VERSION}".tar.gz
sudo tar xf signal-cli-"${VERSION}".tar.gz -C /opt
sudo ln -sf /opt/signal-cli-"${VERSION}"/bin/signal-cli /usr/local/bin/
```
You can find further instructions on the Wiki:
- [Quickstart](https://github.com/AsamK/signal-cli/wiki/Quickstart)
- [DBus Service](https://github.com/AsamK/signal-cli/wiki/DBus-service)

## Usage

For a complete usage overview please read the [man page](https://github.com/AsamK/signal-cli/blob/master/man/signal-cli.1.adoc) and the [wiki](https://github.com/AsamK/signal-cli/wiki).

Important: The USERNAME is your phone number in international format and must include the country calling code. Hence it should start with a "+" sign. (See [Wikipedia](https://en.wikipedia.org/wiki/List_of_country_calling_codes) for a list of all country codes.)

* Register a number (with SMS verification)

        signal-cli -u USERNAME register

  You can register Signal using a land line number. In this case you can skip SMS verification process and jump directly to the voice call verification by adding the `--voice` switch at the end of above register command.

  Registering may require solving a CAPTCHA challenge: [Registration with captcha](https://github.com/AsamK/signal-cli/wiki/Registration-with-captcha)

* Verify the number using the code received via SMS or voice, optionally add `--pin PIN_CODE` if you've added a pin code to your account

        signal-cli -u USERNAME verify CODE

* Send a message

        signal-cli -u USERNAME send -m "This is a message" RECIPIENT

* Pipe the message content from another process.

        uname -a | signal-cli -u USERNAME send RECIPIENT

* Receive messages

        signal-cli -u USERNAME receive

**Hint**: The Signal protocol expects that incoming messages are regularly received (using `daemon` or `receive` command).
This is required for the encryption to work efficiently and for getting updates to groups, expiration timer and other features.

## Storage

The password and cryptographic keys are created when registering and stored in the current users home directory:

        $XDG_DATA_HOME/signal-cli/data/
        $HOME/.local/share/signal-cli/data/

## Building

This project uses [Gradle](http://gradle.org) for building and maintaining
dependencies. If you have a recent gradle version installed, you can replace `./gradlew` with `gradle` in the following steps.

1. Checkout the source somewhere on your filesystem with

        git clone https://github.com/AsamK/signal-cli.git

2. Execute Gradle:

        ./gradlew build

3. Create shell wrapper in *build/install/signal-cli/bin*:

        ./gradlew installDist

4. Create tar file in *build/distributions*:

        ./gradlew distTar

### Building a native binary with GraalVM (EXPERIMENTAL)

It is possible to build a native binary with [GraalVM](https://www.graalvm.org).
This is still experimental and will not work in all situations.

1. [Install GraalVM and setup the enviroment](https://www.graalvm.org/docs/getting-started/#install-graalvm)
2. [Install prerequisites](https://www.graalvm.org/reference-manual/native-image/#prerequisites)
3. Execute Gradle:

        ./gradlew assembleNativeImage

   The binary is available at *build/native-image/signal-cli*

## FAQ and Troubleshooting
For frequently asked questions and issues have a look at the [wiki](https://github.com/AsamK/signal-cli/wiki/FAQ)

## License

This project uses libsignal-service-java from Open Whisper Systems:

https://github.com/WhisperSystems/libsignal-service-java

Licensed under the GPLv3: http://www.gnu.org/licenses/gpl-3.0.html
