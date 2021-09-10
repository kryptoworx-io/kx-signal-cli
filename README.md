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




## Usage
The API is exposed via the interface SignalCLI. 
Example: 
```java
SignalCLI signalCLI = SignalCLIFactory.INSTANCE.getSignalCLI();
signalCLI.send("test123", "+43123123123");
```

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
        
### Publish to local maven repo

```sh
./gradlew publishToMavenLocal
```
